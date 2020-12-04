package com.hzwz.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.Constants;
import com.hzwz.tailbase.Utils;
import com.hzwz.tailbase.backendprocess.CheckSumService;
import com.hzwz.tailbase.backendprocess.TraceIdBatch;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.util.*;

import static com.hzwz.tailbase.Constants.WRONG_TRACE_BATCH;


public class ClientDataSendingThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataSendingThread.class.getName());

    private Jedis jedis = Utils.getJedis();

    public static void start() {
        Thread t = new Thread(new ClientDataSendingThread(), "ClientDataSendingThread");
        t.start();
    }

    private String port = System.getProperty("server.port", "8080");
    private String redisKey =  WRONG_TRACE_BATCH + port;

    @Override
    public void run() {
        String traceIdBatchJson;
        while (true) {
            if (jedis.llen(redisKey) > 0) {
                traceIdBatchJson = jedis.lpop(redisKey);
                TraceIdBatch traceIdBatch = JSON.parseObject(traceIdBatchJson, new TypeReference<TraceIdBatch>() {
                });

                if (traceIdBatch != null) {
                    String result = ClientProcessData.getWrongTracing(traceIdBatch);

                    while (true) {
                        if (jedis.setnx(Constants.REDIS_LOCK_DATA, Thread.currentThread().getName() + port) == 1) {

                            String key = String.format("%s|%s|%s", traceIdBatch.getBatchPos(), traceIdBatch.getThreadNumber(), port);
                            jedis.hset(Constants.WRONG_TRACE_DATA, key, result);

                            LOGGER.info(String.format("sent wrong tracing, result: %s", result));
                            jedis.del(Constants.REDIS_LOCK_DATA);
                            break;
                        }
                    }
                }
            }
        }
    }
}
