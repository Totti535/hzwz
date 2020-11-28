package com.alibaba.tailbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class ClientDataThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataThread.class.getName());

    private long startPos;
    private long currentPartSize;
    private InputStream input;

    public ClientDataThread(long startPos, long currentPartSize, InputStream input) {
        this.startPos = startPos;
        this.currentPartSize = currentPartSize;
        this.input = input;
    }

    public void run() {
        String line;
        long count = 0;
        try {
            this.input.skip(startPos);
            BufferedReader bf = new BufferedReader(new InputStreamReader(this.input));

            int pos = ClientProcessData.POS.getAndIncrement();
            Set<String> badTraceIdList = new HashSet<>(1000);

            currentPartSize = 20000;
            while (count < currentPartSize && (line = bf.readLine()) != null) {
                Map<String, List<String>> traceMap = ClientProcessData.BATCH_TRACE_LIST.get(pos);
                count++;

                String[] cols = line.split("\\|");
                if (cols != null && cols.length > 1) {
                    String traceId = cols[0];
                    List<String> spanList = traceMap.get(traceId);
                    if (spanList == null) {
                        spanList = new ArrayList<>();
                        traceMap.put(traceId, spanList);
                    }
                    spanList.add(line);
                    if (cols.length > 8) {
                        String tags = cols[8];
                        if (tags != null) {
                            if (tags.contains("error=1")) {
                                badTraceIdList.add(traceId);
                            } else if (tags.contains("http.status_code=") && tags.indexOf("http.status_code=200") < 0) {
                                badTraceIdList.add(traceId);
                            }
                        }
                    }
                }
                if (count % Constants.BATCH_SIZE == 0) {
                    pos = ClientProcessData.POS.getAndIncrement();
                    // loop cycle
                    if (pos >= ClientProcessData.BATCH_COUNT) {
                        pos = 0;
                    }

                    if (badTraceIdList.size() > 0) {
                        updateWrongTraceId(badTraceIdList, pos);
                        badTraceIdList.clear();

                        LOGGER.info("suc to updateBadTraceId, batchPos:" + pos);
                    }
                }
            }
            input.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            synchronized (ClientProcessData.lock) {
                ClientProcessData.lock.notify();
            }
        }
    }

    private void updateWrongTraceId(Set<String> badTraceIdList, int batchPos) {
        String json = JSON.toJSONString(badTraceIdList);
        if (badTraceIdList.size() > 0) {
            try {
                LOGGER.info("updateBadTraceId, json:" + json + ", batch:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json).add("batchPos", batchPos + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        }
    }
}
