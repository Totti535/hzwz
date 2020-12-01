package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.tailbase.Constants.NUMBER_OF_THREAD;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // an list of trace map,like ring buffe.  key is traceId, value is spans ,  r
    public static Vector<Map<String,List<String>>> BATCH_TRACE_LIST = new Vector<>();
    // make 50 bucket to cache traceData
    public static int BATCH_COUNT = 15;

    public static Map<Integer, Integer> batchProcessMap = new ConcurrentHashMap<>();

    public static  void init() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        try {
            long startTime = System.currentTimeMillis();

            long dataSize = getSourceDataSize();
            List<InputStream> inputStreams = new ArrayList<>();
            List<BufferedReader> bufferedReaders = new ArrayList<>();;
            long partSize = dataSize / NUMBER_OF_THREAD;

            for (int i = 0; i < NUMBER_OF_THREAD; i++) {
                long start = partSize * i;
                long end = start + partSize + 500;

                boolean isLast = false;
                if (i == NUMBER_OF_THREAD - 1) {
                    isLast = true;
                }

                InputStream input = getSourceDataInputStream(start, end, isLast);
                inputStreams.add(input);
                bufferedReaders.add(new BufferedReader(new InputStreamReader(input)));
            }

            ExecutorService threadPool = Executors.newFixedThreadPool(NUMBER_OF_THREAD);
            List<Future<?>> futures = new ArrayList<>();

            for (BufferedReader bf : bufferedReaders) {
                Future<?> future = threadPool.submit(new ClientDataThread(bf));
                futures.add(future);
            }

            for (Future<?> future : futures) {
                future.get();
            }

            threadPool.shutdown();
            for (BufferedReader bufferedReader : bufferedReaders) {
                bufferedReader.close();
            }

            for (InputStream input : inputStreams) {
                input.close();
            }

            LOGGER.info("Time used(second): " + (double) ((System.currentTimeMillis() - startTime) / 1000));
            callFinish();
        } catch (Exception e) {
            LOGGER.warn("fail to process data", e);
        }
    }

    // notify backend process when client process has finished.
    private void callFinish() {
        try {
            Request request = new Request.Builder().url("http://localhost:8002/finish").build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }


    public static String getWrongTracing(String wrongTraceIdList, int batchPos) {
        LOGGER.info(String.format("getWrongTracing, batchPos:%d, wrongTraceIdList:\n %s" ,
                batchPos, wrongTraceIdList));
        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>(){});
        Map<String,List<String>> wrongTraceMap = new HashMap<>();
        int pos = batchPos % BATCH_COUNT;
        int previous = pos - 1;
        if (previous == -1) {
            previous = BATCH_COUNT -1;
        }
        int next = pos + 1;
        if (next == BATCH_COUNT) {
            next = 0;
        }
        getWrongTraceWithBatch(previous, pos, traceIdList, wrongTraceMap);
        getWrongTraceWithBatch(pos, pos, traceIdList,  wrongTraceMap);
        getWrongTraceWithBatch(next, pos, traceIdList, wrongTraceMap);
        // to clear spans, don't block client process thread. TODO to use lock/notify
        BATCH_TRACE_LIST.get(previous).clear();
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchPos, int pos,  List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(batchPos);
        for (String traceId : traceIdList) {
            List<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross to batch (e.g batch size 20000, span1 in line 19999, span2 in line 20001)
                List<String> existSpanList = wrongTraceMap.get(traceId);
                if (existSpanList != null) {
                    existSpanList.addAll(spanList);
                } else {
                    wrongTraceMap.put(traceId, spanList);
                }
                // output spanlist to check
                String spanListString = spanList.stream().collect(Collectors.joining("\n"));
                LOGGER.info(String.format("getWrongTracing, batchPos:%d, pos:%d, traceId:%s, spanList:\n %s",
                        batchPos, pos,  traceId, spanListString));
            }
        }
    }

    private String getPath(){
        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if ("8001".equals(port)){
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
        } else {
            return null;
        }
    }

    private boolean isDev() {
        return Boolean.valueOf(System.getProperty("is.dev", "false"));
    }

    private InputStream getSourceDataInputStream() throws IOException {
        InputStream input = null;
        if (isDev()) {
            if ("8000".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace1.data");
                input = new FileInputStream(file);
            }

            if ("8001".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace2.data");
                input = new FileInputStream(file);
            }

        } else {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                throw new IOException("path is empty");
            }

            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            input = httpConnection.getInputStream();
        }
        return input;
    }

    private InputStream getSourceDataInputStream(long start, long end, boolean isLast) throws IOException {
        InputStream input = null;
        if (isDev()) {
            if ("8000".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace1.data");
                input = new FileInputStream(file);
            }

            if ("8001".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace2.data");
                input = new FileInputStream(file);
            }

        } else {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                throw new IOException("path is empty");
            }

            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            if (!isLast) {
                httpConnection.setRequestProperty("Range", String.format("bytes=%s-%s"));
            } else {
                httpConnection.setRequestProperty("Range", String.format("bytes=%s-"));
            }
            input = httpConnection.getInputStream();
        }
        return input;
    }

    private long getSourceDataSize() throws IOException {
        if (isDev()) {
            if ("8000".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace1.data");
                return file.length();
            }

            if ("8001".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace2.data");
                return file.length();
            }
        } else {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                throw new IOException("path is empty");
            }

            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            return httpConnection.getContentLengthLong();
        }

        return 0;
    }

}
