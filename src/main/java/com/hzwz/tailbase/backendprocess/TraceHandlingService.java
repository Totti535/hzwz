package com.hzwz.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.Constants;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TraceHandlingService implements Runnable {

    Jedis jedis = new Jedis("localhost", 8003);

    private static final String WRONG_TRACE_ID = "wrong_trace_id";

    private Map<String, String> map = new HashMap<>();

    public static void start() {
        Thread t = new Thread(new TraceHandlingService(), "TraceHandlingServiceThread");
        t.start();
    }

    @Override
    public void run() {
        jedis.del(WRONG_TRACE_ID);

        while (true) {
            if (jedis.setnx(Constants.REDIS_LOCK, Thread.currentThread().getName()) == 0) {
                map = jedis.hgetAll(WRONG_TRACE_ID);

                if (!map.isEmpty()) {
                    jedis.del(WRONG_TRACE_ID);

                    for (Map.Entry<String, String> entry : map.entrySet()) {

                        String[] cols = entry.getKey().split("\\|");
                        int batchPos = Integer.valueOf(cols[0]);
                        int threadNumber = Integer.valueOf(cols[1]);
                        String traceIdListJson = entry.getValue();

                        int pos = batchPos % BackendController.BATCH_COUNT;
                        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
                        });

                        TraceIdBatch traceIdBatch = BackendController.TRACEID_BATCH_LIST.get(threadNumber).get(pos);

                        if (traceIdList != null && traceIdList.size() > 0) {
                            traceIdBatch.setBatchPos(batchPos);
                            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
                            traceIdBatch.getTraceIdList().addAll(traceIdList);
                        }
                    }
                    map.clear();

                    try {
                        Thread.sleep(10);
                    } catch (Exception ex) {

                    }
                }
            }
        }


    }
}
