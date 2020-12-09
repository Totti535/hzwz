package com.alibaba.tailbase.clientprocess;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurlThread implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CurlThread.class.getName());

    private long start;
    private long end;
    private String dest;
    private String url;
    private long totalLength;

    public CurlThread(long start, long end, String dest, String url, long totalLength) {
        this.start = start;
        this.end = end;
        this.dest = dest;
        this.url = url;
        this.totalLength = totalLength;
    }

    @Override
    public Boolean call() {
        try {
            String command =
                    String.format("cd %s && curl -s -r %s-%s %s 2>&1 | awk -F '|' -vOFS='|' '{if (NF>0){if( ($9 ~ /error=1/)||($9 ~ /http.status_code=/ && $9 !~ /http.status_code=200/)){print $1\"_%s-%s\"}}}'",
                            dest,
                            start,
                            end,
                            url,
                            //if this is the first batch, no more up context, else up to the byte length of 21000 rows
                            (start - ClientProcessData.BYTES_OF_20000_ROWS < 0 ? 0 : start - ClientProcessData.BYTES_OF_20000_ROWS),
                            //if this is the last batch, no more down context, else down to the byte length of 21000 rows
                            (end + ClientProcessData.BYTES_OF_20000_ROWS > totalLength ? totalLength : end + ClientProcessData.BYTES_OF_20000_ROWS)
                    );
            //LOGGER.info("Command: " + command);
            String[] commandArr = {"/bin/sh", "-c", command};
            Process p = Runtime.getRuntime().exec(commandArr);

            InputStream is = p.getInputStream();
            if (is != null) {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null) {
                    ClientProcessData.wrongTraceIds.add(line);
                }
            }

            p.waitFor();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("CurlThread Exception.", e.getMessage());
            return false;
        }
    }
}

