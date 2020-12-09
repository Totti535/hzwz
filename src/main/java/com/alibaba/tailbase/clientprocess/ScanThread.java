package com.alibaba.tailbase.clientprocess;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanThread implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanThread.class.getName());

    private String dest;
    private String url;
    private Entry<String, List<String>> e;

    public ScanThread(Entry<String, List<String>> e, String dest, String url) {
        this.e = e;
        this.dest = dest;
        this.url = url;
    }

    public Boolean call() {
        try {
            StringBuffer sb = new StringBuffer();
            List<String> traceIds = e.getValue();

            int i = 1;
            for (String traceId : traceIds) {
                if (i >= traceIds.size()) {
                    sb.append(traceId);
                } else {
                    sb.append(traceId + "|");
                }
                i++;
            }
            String command = String.format("cd %s && curl -s -r %s %s 2>&1 | grep -i -E '%s'", dest, e.getKey(), url, sb.toString());
            String[] commandArr = {"/bin/sh", "-c", command};
            //LOGGER.info("Calling command: " + command);
            Process p = Runtime.getRuntime().exec(commandArr);

            InputStream is = p.getInputStream();
            if (is != null) {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null) {
                    ClientProcessData.allSpans.add(line);
                }
            }

            p.waitFor();
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("SendSpanThread Exception.", e.getMessage());
            return false;
        }
    }
}

