package com.hzwz.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.Constants;
import com.hzwz.tailbase.Utils;
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


public class ClientDataSendingThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataSendingThread.class.getName());

    private Jedis jedis = Utils.getJedis();

    @Override
    public void run() {

        String traceIdBatchJson;
            while (jedis.llen(WRONG_TRACE_BATCH) > 0) {
                traceIdBatchJson = jedis.lpop(WRONG_TRACE_BATCH);
                TraceIdBatch traceIdBatch = JSON.parseObject(traceIdBatchJson, new TypeReference<TraceIdBatch>() {
                });

               //String result =  ClientProcessData.getWrongTracing(traceIdBatch);
            }
    }

}
