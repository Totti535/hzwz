package com.hzwz.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.hzwz.tailbase.Constants;
import com.hzwz.tailbase.Utils;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.util.*;


public class ClientDataThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataThread.class.getName());

    public static final Object lock = new Object();

    private int threadNumber;

    private BufferedReader bf;

    private long partSize;

    private static Jedis jedis = Utils.getJedis();

    private String port = System.getProperty("server.port", "8080");

    public ClientDataThread(int threadNumber, BufferedReader bf, long partSize) {
        this.threadNumber = threadNumber;
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

                Map<String, List<String>> traceMap = ClientProcessData.BATCH_TRACE_LIST.get(threadNumber).get(pos);
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
                    traceMap = ClientProcessData.BATCH_TRACE_LIST.get(threadNumber).get(pos);
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
                    //updateWrongTraceId(badTraceIdList, batchPos, threadNumber);

                    updateWrongTraceIdRedis(badTraceIdList, batchPos, threadNumber);
                    badTraceIdList.clear();
                }
            }
            //updateWrongTraceId(badTraceIdList, (int) count / Constants.BATCH_SIZE - 1, threadNumber);
            updateWrongTraceIdRedis(badTraceIdList, (int) count / Constants.BATCH_SIZE - 1, threadNumber);
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
    private void updateWrongTraceId(Set<String> badTraceIdList, int batchPos, int threadNumber) {
        String json = JSON.toJSONString(badTraceIdList);
        if (badTraceIdList.size() > 0) {
            try {
                LOGGER.info("updateBadTraceId, json:" + json + ", batch:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json)
                        .add("batchPos", batchPos + "")
                        .add("threadNumber", threadNumber + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
                LOGGER.info("suc to updateBadTraceId, batchPos:" + batchPos + "thread number:" + threadNumber);
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos + "thread number:" + threadNumber);
            }
        }
    }

    private void updateWrongTraceIdRedis(Set<String> badTraceIdList, int batchPos, int threadNumber) {
        String json = JSON.toJSONString(badTraceIdList);
        String redisKey = Constants.WRONG_TRACE_ID;

        while (true) {
            if (jedis.setnx(Constants.REDIS_LOCK, Thread.currentThread().getName() + port) == 1) {

                String key = String.format("%s|%s|%s", batchPos, threadNumber, port);
                jedis.hset(redisKey, key, json);

                LOGGER.info(String.format("set wrong trace id, batchPos: %s, threadNumber: %s, port: %s", batchPos, threadNumber, port));
                jedis.del(Constants.REDIS_LOCK);
                break;
            }
        }
    }
}
