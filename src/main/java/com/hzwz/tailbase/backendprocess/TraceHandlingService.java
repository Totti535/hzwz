package com.hzwz.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.Constants;
import com.hzwz.tailbase.Utils;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hzwz.tailbase.Constants.WRONG_TRACE_ID;
import static com.hzwz.tailbase.Constants.REDIS_LOCK;


public class TraceHandlingService implements Runnable {

    private Map<String, String> map = new HashMap<>();

    public static void start() {
        Thread t = new Thread(new TraceHandlingService(), "TraceHandlingServiceThread");
        t.start();
    }

    @Override
    public void run() {
        Jedis jedis = Utils.getJedis();
        jedis.del(WRONG_TRACE_ID);
        jedis.del(REDIS_LOCK);

        while (true) {
            if (jedis.setnx(REDIS_LOCK, Thread.currentThread().getName()) == 1) {
                map = jedis.hgetAll(WRONG_TRACE_ID);

                if (!map.isEmpty()) {
                    jedis.del(WRONG_TRACE_ID);
                }

                jedis.del(REDIS_LOCK);

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
                }catch (Exception ex) {

                }
            }
        }
    }
}
