package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.tailbase.clientprocess.ClientProcessData.IS_READ_FINISHED;
import static com.alibaba.tailbase.clientprocess.ClientProcessData.SOURCE_DATA_QUEUE;


public class ClientDataHandler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientDataHandler.class.getName());

    private static String port = System.getProperty("server.port");

    public static Map<Integer, Map<String, List<String>>> FULL_DATA = new ConcurrentHashMap<>();;


    public static void start() {
        Thread t = new Thread(new ClientDataHandler(), "ClientDataHandlerThread");
        t.start();
    }

    @Override
    public void run() {
        try {
            long count = 0;
            int batchId = 0;
            Set<String> badTraceIdList = new HashSet<>(1000);

            InputStream input = getSourceDataInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            String line;
            while ((line = bf.readLine()) != null) {
                count++;

                Map<String, List<String>> traceMap = FULL_DATA.get(batchId);
                if (traceMap == null) {
                    traceMap = new ConcurrentHashMap<>();
                    FULL_DATA.put(batchId, traceMap);
                }

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
                    updateWrongTraceId(badTraceIdList, batchId);

                    if (count >= Constants.BATCH_SIZE * 2) {
                        notifyBackend(batchId - 1);
                    }

                    badTraceIdList.clear();
                    LOGGER.info("suc to updateBadTraceId, batchPos:" + batchId);
                    batchId++;

                }
            }
            updateWrongTraceId(badTraceIdList, batchId);
            bf.close();
            input.close();
            callFinish();
        } catch (Exception e) {
            LOGGER.warn("fail to process data", e);
        }
    }

    private void notifyBackend(int batchId) {
        try {
            LOGGER.info("notify backend, batchId:" + batchId);
            RequestBody body = new FormBody.Builder()
                    .add("batchId", batchId + "").build();
            Request request = new Request.Builder().url("http://localhost:8002/notifyToGetData").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();

            LOGGER.info("finish to notify backend, batchId:" + batchId);
        } catch (Exception e) {
            LOGGER.warn("notify backend, batchId:" + batchId);
        }
    }


    private void updateWrongTraceId(Set<String> badTraceIdList, int batchId) {
        String json = JSON.toJSONString(badTraceIdList);
        if (badTraceIdList.size() > 0) {
            try {
                LOGGER.info("updateBadTraceId, json:" + json + ", batch:" + batchId);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json)
                        .add("batchId", batchId + "")
                        .add("port", port)
                        .build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchId);
            }
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


    public static String getWrongTracing(String wrongTraceIdList, int batchId) {
        LOGGER.info(String.format("getWrongTracing, batchId:%d, wrongTraceIdList:\n %s" ,
                batchId, wrongTraceIdList));

        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>(){});
        Map<String,List<String>> wrongTraceMap = new HashMap<>();
        getWrongTraceWithBatch(batchId, traceIdList, wrongTraceMap);
        getWrongTraceWithBatch(batchId + 1, traceIdList,  wrongTraceMap);

        FULL_DATA.remove(batchId);
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchId, List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = FULL_DATA.get(batchId);
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
                LOGGER.info(String.format("getWrongTracing, batchPos:%d, traceId:%s, spanList:\n %s",
                        batchId, traceId, spanListString));
            }
        }
    }

    private String getPath() {
        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if ("8001".equals(port)) {
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


}
