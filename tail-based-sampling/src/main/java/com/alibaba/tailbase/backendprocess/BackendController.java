package com.alibaba.tailbase.backendprocess;

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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@RestController
public class BackendController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    private static Map<String, String> TRACE_CHECKSUM_MAP = new ConcurrentHashMap<>();

    // FINISH_PROCESS_COUNT will add one, when process call finish();
    private static volatile Integer FINISH_PROCESS_COUNT = 0;

    private static Map<Integer, TraceIdBatch> BAD_TRACE_ID_MAP;

    public static void init() {
        BAD_TRACE_ID_MAP = new HashMap<>();
    }

    @RequestMapping("/setWrongTraceId")
    public String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam Integer batchId, @RequestParam Integer port) {
        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
        });

        if (traceIdList == null || !(traceIdList.size() > 0)) {
            return "nothing to set.";
        }

        LOGGER.info(String.format("setWrongTraceId had been called, batchId:%d, port:%s, traceIdList:%s", batchId, port, traceIdListJson));

        TraceIdBatch traceIdBatch = BAD_TRACE_ID_MAP.get(batchId);
        if (traceIdBatch == null) {
            traceIdBatch = new TraceIdBatch();
        }
        traceIdBatch.getPorts().add(port);
        traceIdBatch.getTraceIdList().addAll(traceIdList);

        BAD_TRACE_ID_MAP.put(batchId, traceIdBatch);
        return "suc";
    }

    @RequestMapping("/notifyToGetData")
    public String notifyToGetData(@RequestParam Integer batchId) {
        TraceIdBatch traceIdBatch = BAD_TRACE_ID_MAP.get(batchId);

        if (traceIdBatch == null || !traceIdBatch.isReady()) {
            LOGGER.info(String.format("batch id: %s is not yet read to get data.", batchId));
            return "not ready.";
        }

        handleWrongTrace(batchId, traceIdBatch);

        return "suc";
    }

    @RequestMapping("/finish")
    public String finish() {
        FINISH_PROCESS_COUNT++;

        LOGGER.info("receive call 'finish', count:" + FINISH_PROCESS_COUNT);

        if (FINISH_PROCESS_COUNT == Constants.CLIENT_DATA_PORTS.length) {

            for (Integer batchId : BAD_TRACE_ID_MAP.keySet()) {
                //handle the rest of trace id in the map.
                handleWrongTrace(batchId, BAD_TRACE_ID_MAP.get(batchId));
            }
        }

        sendCheckSum();
        return "suc";
    }

    private void handleWrongTrace(Integer batchId, TraceIdBatch traceIdBatch) {
        Map<String, Set<String>> map = new HashMap<>();
        for (String port : Constants.CLIENT_DATA_PORTS) {
            Map<String, List<String>> processMap = getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), port, batchId);
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
        LOGGER.info("getWrong batchId:" + batchId + ", traceIdsize:" + traceIdBatch.getTraceIdList().size() + ",result:" + map.size());

        BAD_TRACE_ID_MAP.remove(batchId);

        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            String traceId = entry.getKey();
            Set<String> spanSet = entry.getValue();
            // order span with startTime
            String spans = spanSet.stream().sorted(
                    Comparator.comparing(BackendController::getStartTime)).collect(Collectors.joining("\n"));
            spans = spans + "\n";
            // output all span to check
            // LOGGER.info("traceId:" + traceId + ",value:\n" + spans);
            TRACE_CHECKSUM_MAP.put(traceId, Utils.MD5(spans));
        }

    }

    private Map<String, List<String>> getWrongTrace(@RequestParam String traceIdList, String port, int batchId) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("traceIdList", traceIdList).add("batchPos", batchId + "").build();
            String url = String.format("http://localhost:%s/getWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String, List<String>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<String>>>() {
                    });
            response.close();
            return resultMap;
        } catch (Exception e) {
            LOGGER.warn(String.format("fail to getWrongTrace, batchId: %s, port: %s, json: %s", batchId, port, traceIdList), e);
        }
        return null;
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
