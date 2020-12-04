package com.hzwz.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.CommonController;
import com.hzwz.tailbase.Constants;
import com.hzwz.tailbase.Utils;
import com.hzwz.tailbase.clientprocess.ClientProcessData;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.hzwz.tailbase.Constants.*;

public class CheckSumService implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // save chuckSum for the total wrong trace
    private static Map<String, String> TRACE_CHUCKSUM_MAP = new ConcurrentHashMap<>();

    private Jedis jedis = Utils.getJedis();

    public static void start() {
        Thread t = new Thread(new CheckSumService(), "CheckSumServiceThread");
        t.start();
    }

    @Override
    public void run() {
        List<TraceIdBatch> traceIdBatchs = new ArrayList<>();
        String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};
        while (true) {
            try {
                traceIdBatchs = BackendController.getFinishedBatch();
                if (traceIdBatchs.isEmpty()) {
                    // send checksum when client process has all finished.
                    if (BackendController.isFinished()) {
                        if (sendCheckSum()) {
                            break;
                        }
                    }
                    continue;
                }
/*
                CountDownLatch cdl = new CountDownLatch(traceIdBatchs.size());
                Map<String, Set<String>> map = new ConcurrentHashMap<>();
                for (TraceIdBatch traceIdBatch : traceIdBatchs) {
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            // if (traceIdBatch.getTraceIdList().size() > 0) {
                            int batchPos = traceIdBatch.getBatchPos();
                            // to get all spans from remote
                            for (String port : ports) {
                                Map<String, List<String>> processMap =
                                        getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), port, batchPos, traceIdBatch.getThreadNumber());
                                if (processMap != null) {
                                    for (Map.Entry<String, List<String>> entry : processMap.entrySet()) {
                                        String traceId = entry.getKey();
                                        Set<String> spanSet = map.get(traceId);
                                        if (spanSet == null) {
                                            spanSet = new HashSet<>();
                                            map.put(traceId, spanSet);
                                        }
                                        spanSet.addAll(entry.getValue());
                                    }
                                }
                            }
                            LOGGER.info(String.format("getWrong batchPos: %s, thread number: %s, traceIdsize: %s, result: %s",
                                    batchPos, traceIdBatch.getThreadNumber(), traceIdBatch.getTraceIdList().size(), map.size()));
                            cdl.countDown();
                        }
                    });
                    thread.start();
                }

                cdl.await();
 */
                Map<String, Set<String>> map = new ConcurrentHashMap<>();
                while (true) {
                    if (jedis.setnx(Constants.REDIS_LOCK_DATA, Thread.currentThread().getName()) == 1) {

                        Map<String, String> redisMap = jedis.hgetAll(WRONG_TRACE_DATA);

                        if (!redisMap.isEmpty()) {
                            for (String key : redisMap.keySet()) {
                                Map<String, List<String>> processMap = JSON.parseObject(redisMap.get(key),
                                        new TypeReference<Map<String, List<String>>>() {
                                        });

                                LOGGER.info(String.format("get wrong tracing, key: %s, result: %s", key, processMap.size()));

                                if (processMap != null) {
                                    for (Map.Entry<String, List<String>> entry : processMap.entrySet()) {
                                        String traceId = entry.getKey();
                                        Set<String> spanSet = map.get(traceId);
                                        if (spanSet == null) {
                                            spanSet = new HashSet<>();
                                            map.put(traceId, spanSet);
                                        }
                                        spanSet.addAll(entry.getValue());
                                    }
                                }
                            }
                        }

                        jedis.del(WRONG_TRACE_DATA);
                        jedis.del(Constants.REDIS_LOCK_DATA);
                        break;
                    }
                }

                for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                    String traceId = entry.getKey();
                    Set<String> spanSet = entry.getValue();
                    // order span with startTime
                    String spans = spanSet.stream().sorted(
                            Comparator.comparing(CheckSumService::getStartTime)).collect(Collectors.joining("\n"));
                    spans = spans + "\n";
                    // output all span to check
                    // LOGGER.info("traceId:" + traceId + ",value:\n" + spans);
                    TRACE_CHUCKSUM_MAP.put(traceId, Utils.MD5(spans));
                }

            } catch (Exception e) {
                LOGGER.warn("fail to getWrongTrace", e);
            } finally {
                if (traceIdBatchs.isEmpty()) {
                    try {
                        Thread.sleep(10);
                    } catch (Throwable e) {
                        // quiet
                    }
                }
            }
        }
    }

    /**
     * call client process, to get all spans of wrong traces.
     *
     * @param traceIdList
     * @param port
     * @param batchPos
     * @return
     */
    private Map<String, List<String>> getWrongTrace(@RequestParam String traceIdList, String port, int batchPos, int threadNumber) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("traceIdList", traceIdList)
                    .add("batchPos", batchPos + "")
                    .add("threadNumber", threadNumber + "")
                    .build();
            String url = String.format("http://localhost:%s/getWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String, List<String>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<String>>>() {
                    });
            response.close();
            return resultMap;
        } catch (Exception e) {
            LOGGER.warn("fail to getWrongTrace, json:" + traceIdList + ",batchPos:" + batchPos + "threadNumber: " + threadNumber, e);
        }
        return null;
    }


    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(TRACE_CHUCKSUM_MAP);
            RequestBody body = new FormBody.Builder()
                    .add("result", result).build();
            String url = String.format("http://localhost:%s/api/finished", CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                LOGGER.warn("suc to sendCheckSum, result:" + result);
                return true;
            }
            LOGGER.warn("fail to sendCheckSum:" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            LOGGER.warn("fail to call finish", e);
        }
        return false;
    }

    public static long getStartTime(String span) {
        if (span != null) {
            String[] cols = span.split("\\|");
            if (cols.length > 8) {
                return Utils.toLong(cols[1], -1);
            }
        }
        return -1;
    }


}
