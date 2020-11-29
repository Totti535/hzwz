package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Entry;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // an list of trace map,like ring buffe.  key is traceId, value is spans ,  r
    private static List<Map<String, List<String>>> BATCH_TRACE_LIST = new ArrayList<>();

    public static Queue<String> SOURCE_DATA_QUEUE = new LinkedList<>();

    // make 50 bucket to cache traceData
    private static int BATCH_COUNT = 15;

    private static String port;

    public static Map<Integer, List<Entry>> fullData;

    public static void init() {
        // for (int i = 0; i < BATCH_COUNT; i++) {
        //BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        //}
        fullData = new HashMap<>();
        port = System.getProperty("server.port");
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        try {
            InputStream input = getSourceDataInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            String line;
            int batchSize = 20000;
            int batchCount = 0;

            int count = 1;
            boolean firstRound = true;

            while ((line = bf.readLine()) != null) {

                List<Entry> batchData = fullData.get(batchCount);
                if (batchData == null) {
                    batchData = new ArrayList<Entry>();
                }
                String[] cols = line.split("\\|");
                String traceId = cols[0];
                Boolean isWrong = false;
                if (cols.length > 8) {
                    String tags = cols[8];
                    if (tags != null) {
                        isWrong = tags.contains("error=1") || (tags.contains("http.status_code=") && tags.indexOf("http.status_code=200") < 0);
                    }
                }
                Entry e = new Entry();
                e.setSpan(line);
                e.setTraceId(traceId);
                e.setIsWrong(isWrong);
                batchData.add(e);
                fullData.put(batchCount, batchData);

                if (count % 20000 == 0) {
                    updateWrongTraceId(batchCount);

                    if (count >= 40000) {
                        notifyBackend(batchCount - 1);
                    }
                    batchCount++;
                }

                count++;
            }
            bf.close();
            input.close();
            callFinish();
        } catch (Exception e) {
            LOGGER.warn("fail to process data", e);
        }
    }

    private void updateWrongTraceId( int batchCount) {
        //list the first 20000 Wrong trace id
        List<Entry> batchData = fullData.get(batchCount);
        List<Entry> badEntryList = batchData.stream().filter(entry -> true == entry.getIsWrong()).collect(Collectors.toList());
        Set<String> badTraceIdList = badEntryList.stream().map(Entry::getTraceId).distinct().collect(Collectors.toSet());
        //sending first batch to blue block
        updateWrongTraceId(badTraceIdList, batchCount);

        LOGGER.info("suc to updateBadTraceId, batchCount:" + batchCount);
    }

    private void notifyBackend(int batchId) {
        try {
            LOGGER.info("notify backend, batchId:" + batchId);
            RequestBody body = new FormBody.Builder()
                    .add("batchId", batchId + "").build();
            Request request = new Request.Builder().url("http://localhost:8002/notifyToGetData").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();

            fullData.remove(batchId);

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
        LOGGER.info(String.format("getWrongTracing, batchId:%d, wrongTraceIdList:\n %s",
                batchId, wrongTraceIdList));

        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>() {
        });

        Map<String, List<String>> wrongTraceMap = new HashMap<>();

        List<Entry> current = fullData.get(batchId);
        List<Entry> next = fullData.get(batchId + 1);

        List<Entry> result = new ArrayList<>();
        result.addAll(current.stream().filter(entry -> traceIdList.contains(entry.getTraceId())).collect(Collectors.toList()));
        result.addAll(next.stream().filter(entry -> traceIdList.contains(entry.getTraceId())).collect(Collectors.toList()));

        for (Entry entry : result) {
            if (wrongTraceMap.get(entry.getTraceId()) == null) {
                List<String> list = new ArrayList<>();
                list.add(entry.getSpan());
                wrongTraceMap.put(entry.getTraceId(), list);
            }else {
                wrongTraceMap.get(entry.getTraceId()).add(entry.getSpan());
            }
        }
        return JSON.toJSONString(wrongTraceMap);
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
