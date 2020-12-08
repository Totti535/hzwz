package com.alibaba.tailbase.clientprocess;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;


import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.util.StringUtils;

public class SendSpanThread implements Callable<Boolean>{
    private static final Logger LOGGER = LoggerFactory.getLogger(SendSpanThread.class.getName());

    private String dest;
    private Entry<String, List<String>> traceIdEntry;

    public SendSpanThread(String dest, Entry<String, List<String>> traceIdEntry) {
        this.dest = dest;
        this.traceIdEntry = traceIdEntry;
    }

    public Boolean call() {
        try {

            String traceId = traceIdEntry.getKey();
            List<String> prop = traceIdEntry.getValue();
            //filter 2 yellow block same file has wrong trace id
            Set<String> fileIdxes = new HashSet<String>(prop);
            Iterator<String> it = fileIdxes.iterator();

            while (it.hasNext()) {
                int fileIdx = Integer.valueOf(it.next());

                //防止上下文遗漏
                String command = String.format("cd %s && awk -F '|' -vOFS='|' '{if (NF>0) {if($1 ~ /%s/){ print $0 >> \"wrong.data\"}}}'", dest, traceId);
//                String command = String.format("cd %s && awk -F '|' -vOFS='|' '{if (NF>0) {if($1 ~ /%s/){ print $0 >> $1\".data\"}}}'", dest, traceId);

                String file1 = "";
                String file2 = "";
                String file3 = "";

                if(fileIdx - 1 < 1) {
                }else {
                    file1 = String.format(" part_%s.data", fileIdx-1);
                }

                file2 = String.format(" part_%s.data", fileIdx);

                if(fileIdx + 1 > 100) {
                }else {
                    file3 = String.format(" part_%s.data", fileIdx+1);
                }

                if(!StringUtils.isEmpty(file1)) {
                    command = command + file1;
                }
                command = command + file2;

                if(!StringUtils.isEmpty(file3)) {
                    command = command + file3;
                }

                String[] commandArr = {"/bin/sh", "-c", command};
                File f = new File(dest + traceId + ".data");
                LOGGER.info("Calling command: " + command);
                if(!f.exists()) {
                    Process p = Runtime.getRuntime().exec(commandArr);
                    p.waitFor();
                }

            }

//            Map<String, Set<String>> spansMap = new HashMap<String, Set<String>>();
//            File f = new File(dest + traceId + ".data");
//            if(f.exists()) {
//                List<String> lines = FileUtils.readLines(f, "UTF-8");
//                Set<String> spansSet = new HashSet<String>(lines);
//                spansMap.put(traceId, spansSet);
//                setWrongTraceMap(spansMap);
//            }
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("SendSpanThread Exception.", e.getMessage());
            return false;
        }
    }

    private void setWrongTraceMap(Map<String, Set<String>> wrongTraceMap) {
        String json = JSON.toJSONString(wrongTraceMap);
        String port = System.getProperty("server.port", "8080");
        try {
            RequestBody body = new FormBody.Builder()
                    .add("wrongTraceMap", json)
                    .add("port", port).build();
            Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceMap").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to setWrongTraceMap", e);
        }
    }
}

