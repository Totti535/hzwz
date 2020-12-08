package com.alibaba.tailbase.backendprocess;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@RestController
public class BackendController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());
    public Map<String, String> TRACE_CHECKSUM_MAP = new ConcurrentHashMap<String, String>();
    public Map<String, List<String>> wrongTradeIds = new ConcurrentHashMap<String, List<String>>();
    public Map<String, Set<String>> resultMap = new ConcurrentHashMap<String, Set<String>>();
    public volatile Integer FINISH_PROCESS_COUNT = 0;
    public volatile Integer UPDATED_COUNT = 0;
    public static void init() {

    }

    public static AtomicBoolean block = new AtomicBoolean(true);

    @RequestMapping("/updateWrongTraceId")
    private String updateWrongTraceId(@RequestParam String wrongTradeIdsStr) {
        try {
            List<String> list = JSON.parseObject(wrongTradeIdsStr, new TypeReference<List<String>>(){});

            if(!CollectionUtils.isEmpty(list)) {
                list.stream().forEach(str ->{
                    String[] a = str.split("_");
                    String traceId = a[0];
                    String fileIdx = a[1];

                    List<String> prop = wrongTradeIds.get(traceId);
                    if(prop == null){
                        prop = new ArrayList<String>();
                        prop.add(fileIdx);
                        wrongTradeIds.put(traceId, prop);
                    } else {
                        prop.add(fileIdx);
                        wrongTradeIds.put(traceId, prop);
                    }
                });
            }
            UPDATED_COUNT ++;
            LOGGER.info(String.format("updateWrongTraceId had been called, wrongTradeIds:%s", wrongTradeIds.size()));
        } catch (Exception e) {
            LOGGER.error("updateWrongTraceId failed", e.getMessage());
        }
        return "suc";
    }

    @RequestMapping("/getWrongTraceIds")
    private String getWrongTraceIds() {
        while (true) {
            try {
                Thread.sleep(10);
                if(UPDATED_COUNT == 2) {
                    break;
                }
            } catch (Exception e) {}
        }
        return JSON.toJSONString(wrongTradeIds);
    }


    @RequestMapping("/setWrongTraceMap")
    public String setWrongTraceMap(@RequestParam String wrongTraceMap, @RequestParam Integer port) {
        Map<String, Set<String>> processMap = JSON.parseObject(wrongTraceMap, new TypeReference<Map<String, Set<String>>>() {});
        if(processMap == null || CollectionUtils.isEmpty(processMap)) {
            return "nothing to setWrongTraceMap.";
        }
        for (Map.Entry<String, Set<String>> entry : processMap.entrySet()) {
            String traceId = entry.getKey();
            Set<String> spanSet = resultMap.get(traceId);
            if (spanSet == null) {
                spanSet = new HashSet<>();
                resultMap.put(traceId, spanSet);
            }
            spanSet.addAll(entry.getValue());
        }
        return "suc";
    }

    @RequestMapping("/finish")
    public String finish(@RequestParam Integer port) throws Exception {
        FINISH_PROCESS_COUNT++;
        if (FINISH_PROCESS_COUNT == 2) {
            LOGGER.warn("receive call 'finish', count:" + FINISH_PROCESS_COUNT);
            for (Map.Entry<String, Set<String>> entry : resultMap.entrySet()) {
                String traceId = entry.getKey();
                Set<String> spanSet = entry.getValue();
                // order span with startTime
                String spans = spanSet.stream().sorted(Comparator.comparing(BackendController::getStartTime)).collect(Collectors.joining("\n"));
                spans = spans + "\n";

                TRACE_CHECKSUM_MAP.put(traceId, Utils.MD5(spans));
            }
            sendCheckSum();
        }
        return "suc";
    }

    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(TRACE_CHECKSUM_MAP);
            //LOGGER.info(result);
            RequestBody body = new FormBody.Builder().add("result", result).build();
            String url = String.format("http://localhost:%s/api/finished", CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                //LOGGER.warn("suc to sendCheckSum");
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





