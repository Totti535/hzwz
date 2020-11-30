package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT1;
import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT2;

public class CheckSumService implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // save chuckSum for the total wrong trace
    private static Map<String, String> TRACE_CHECKSUM_MAP = new ConcurrentHashMap<>();

    public static Map<String, List<List<String>>> TRACE_CHECKSUM_MAP_RAW = new ConcurrentHashMap<>();

    public static void start() {
        Thread t = new Thread(new CheckSumService(), "CheckSumServiceThread");
        t.start();
    }

    @Override
    public void run() {
        TraceIdBatch traceIdBatch = null;
        String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};
        while (true) {
            try {
                traceIdBatch = BackendController.getFinishedBatch();
                if (traceIdBatch == null) {
                    // send checksum when client process has all finished.
                    if (BackendController.isFinished()) {
                        if (sendCheckSum()) {
                            break;
                        }
                    }
                    continue;
                }

                for (String port : ports) {
                    setWrongTraceIdBatch(traceIdBatch, port);
                }

                for (Map.Entry<String, List<List<String>>> entry : TRACE_CHECKSUM_MAP_RAW.entrySet()) {
                    String traceId = entry.getKey();
                    List<List<String>> spanList = entry.getValue();

                    if (spanList.size() >= Constants.PROCESS_COUNT) {
                        Set<String> spanSet = new HashSet<>();
                        for (List<String> list : spanList) {
                            spanSet.addAll(list);
                        }
                        // order span with startTime
                        String spans = spanSet.stream().sorted(
                                Comparator.comparing(CheckSumService::getStartTime)).collect(Collectors.joining("\n"));
                        spans = spans + "\n";

                        TRACE_CHECKSUM_MAP.put(traceId, Utils.MD5(spans));
                        TRACE_CHECKSUM_MAP_RAW.remove(entry.getKey());
                    }
                }

            } catch (Exception e) {
                // record batchPos when an exception  occurs.
                int batchPos = 0;
                if (traceIdBatch != null) {
                    batchPos = traceIdBatch.getBatchPos();
                }
                LOGGER.warn(String.format("fail to getWrongTrace, batchPos:%d", batchPos), e);
            } finally {
                if (traceIdBatch == null) {
                    try {
                        Thread.sleep(100);
                    } catch (Throwable e) {
                        // quiet
                    }
                }
            }
        }
    }


    private void setWrongTraceIdBatch(TraceIdBatch traceIdBatch, String port) {
        String json = JSON.toJSONString(traceIdBatch);
        try {
            LOGGER.info("send wrong trace id batch, json:" + json);
            RequestBody body = new FormBody.Builder()
                    .add("batch", json).build();

            String url = String.format("http://localhost:%s/setWrongTraceIdBatch", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to send wrong trace id batch");
        }
    }

    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(TRACE_CHECKSUM_MAP);
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
