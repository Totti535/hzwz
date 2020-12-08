package com.alibaba.tailbase.clientprocess;

import java.io.File;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurlThread implements Callable<Boolean>{
    private static final Logger LOGGER = LoggerFactory.getLogger(CurlThread.class.getName());

    private long start;
    private long end;
    private String dest;
    private String url;
    private int threadNumber;

    public CurlThread(long start, long end, String dest, String url, int threadNumber) {
        this.start = start;
        this.end = end;
        this.dest = dest;
        this.url = url;
        this.threadNumber = threadNumber;
    }


    @Override
    public Boolean call() {
        try {
            String command = "";
//            if(ClientProcessData.isDev()) {
//                command = String.format("curl -r %s-%s -o %s\\part_%s.data %s", start, end, dest, threadNumber, url);
//            }else {
                command = String.format("curl -r %s-%s -o %spart_%s.data %s", start, end, dest, threadNumber, url);
//            }
//            LOGGER.info("calling command: " + command);
            File f = new File(dest + "part_"+ threadNumber + ".data");
            if(!f.exists()) {
                Process p = Runtime.getRuntime().exec(command);
                p.waitFor();

                //split wrong trade id
                String command2 =
                        String.format("cd %s && awk -F '|' -vOFS='|' '{if (NF>0){if( ($9 ~ /error=1/)||($9 ~ /http.status_code=/ && $9 !~ /http.status_code=200/)){ print \"0\" >> $1\"_%s_error.data\"}}}' part_%s.data", dest, threadNumber, threadNumber);

                String[] command2Arr = {"/bin/sh", "-c", command2};
//                LOGGER.info("calling command2: " + command2);
                Process p2 = Runtime.getRuntime().exec(command2Arr);
                p2.waitFor();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("CurlThread Exception.", e.getMessage());
            return false;
        }
    }
}
