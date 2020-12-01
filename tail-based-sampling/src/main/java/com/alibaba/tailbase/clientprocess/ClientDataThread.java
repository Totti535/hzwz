package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.*;

import static com.alibaba.tailbase.clientprocess.ClientProcessData.isDev;


public class ClientDataThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataThread.class.getName());

    public static final Object lock = new Object();

    private BufferedReader bf;

    private long partSize;

    public ClientDataThread(BufferedReader bf, long partSize) {
        this.bf = bf;
        this.partSize = partSize;
    }

    @Override
    public void run() {
        String line;
        int pos = 0;
        long count = 0;
        Set<String> badTraceIdList = new HashSet<>(1000);

        long byteRead = 0;
        try {
            while ((line = bf.readLine()) != null && byteRead <= partSize) {

                byteRead += line.getBytes().length;
                count++;

                Map<String, List<String>> traceMap = ClientProcessData.BATCH_TRACE_LIST.get(pos);
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
                    pos++;
                    // loop cycle
                    if (pos >= ClientProcessData.BATCH_COUNT) {
                        pos = 0;
                    }
                    traceMap = ClientProcessData.BATCH_TRACE_LIST.get(pos);
                    // donot produce data, wait backend to consume data
                    // TODO to use lock/notify
                    if (traceMap.size() > 0) {
                        while (true) {
                            Thread.sleep(10);
                            if (traceMap.size() == 0) {
                                break;
                            }
                        }
                    }
                    int batchPos = (int) count / Constants.BATCH_SIZE - 1;
                    updateWrongTraceId(badTraceIdList, batchPos);
                    badTraceIdList.clear();
                }
            }
            updateWrongTraceId(badTraceIdList, (int) count / Constants.BATCH_SIZE - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * call backend controller to update wrong tradeId list.
     *
     * @param badTraceIdList
     * @param batchPos
     */
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
                LOGGER.info("suc to updateBadTraceId, batchPos:" + batchPos);
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        }
    }
}
