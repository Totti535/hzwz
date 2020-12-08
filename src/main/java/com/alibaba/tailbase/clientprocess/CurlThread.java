package com.alibaba.tailbase.clientprocess;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurlThread implements Callable<Boolean>{
    private static final Logger LOGGER = LoggerFactory.getLogger(CurlThread.class.getName());

    private long start;
    private long end;
    private String dest;
    private String url;

    public CurlThread(long start, long end, String dest, String url) {
        this.start = start;
        this.end = end;
        this.dest = dest;
        this.url = url;
    }

    @Override
    public Boolean call() {
        try {
            //curl -r 0-255970419 http://localhost:8080/trace1.data 2>&1 | awk -F '|' -vOFS='|' '{if (NF>0){if( ($9 ~ /error=1/)||($9 ~ /http.status_code=/ && $9 !~ /http.status_code=200/)){ print '0' >> $1}}}'
            String command = String.format("cd %s && curl -s -r %s-%s %s 2>&1 | awk -F '|' -vOFS='|' '{if (NF>0){if( ($9 ~ /error=1/)||($9 ~ /http.status_code=/ && $9 !~ /http.status_code=200/)){print '0' >> $1}}}'", dest, start, end, url);
            LOGGER.info("Command: " + command);
            String[] commandArr = {"/bin/sh", "-c", command};
            Process p = Runtime.getRuntime().exec(commandArr);
            p.waitFor();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("CurlThread Exception.", e.getMessage());
            return false;
        }
    }
}

