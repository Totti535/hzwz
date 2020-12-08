package com.alibaba.tailbase.clientprocess;

import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanThread implements Callable<Boolean>{
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanThread.class.getName());

    private long start;
    private long end;
    private String dest;
    private String url;
    private List<String> grepTraceIds;
    private int threadNumber;

    public ScanThread(long start, long end, String dest, String url, List<String> grepTraceIds, int threadNumber) {
        this.start = start;
        this.end = end;
        this.dest = dest;
        this.url = url;
        this.grepTraceIds = grepTraceIds;
        this.threadNumber = threadNumber;
    }

    public Boolean call() {
        try {
            for (String string : grepTraceIds) {
                String command = String.format("cd %s && curl -s -r %s-%s %s 2>&1 | grep -i -E '%s' >> spans_%s.data", dest, start, end, url, string, this.threadNumber);
                String[] commandArr = {"/bin/sh", "-c", command};
                LOGGER.info("Calling command: " + command);
                Process p = Runtime.getRuntime().exec(commandArr);
                p.waitFor();
            }
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("SendSpanThread Exception.", e.getMessage());
            return false;
        }
    }
}

