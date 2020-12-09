package com.alibaba.tailbase.backendprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@RestController
public class BackendController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    public Map<String, List<String>> wrong_8000 = new ConcurrentHashMap<String, List<String>>();
    public Map<String, List<String>> wrong_8001 = new ConcurrentHashMap<String, List<String>>();
    public Map<String, List<String>> wrong_return_8000 = new ConcurrentHashMap<String, List<String>>();
    public Map<String, List<String>> wrong_return_8001 = new ConcurrentHashMap<String, List<String>>();

    public Map<String, Set<String>> resultMap = new ConcurrentHashMap<String, Set<String>>();
    public Map<String, String> TRACE_CHECKSUM_MAP = new ConcurrentHashMap<String, String>();
    public volatile Integer FINISH_PROCESS_COUNT = 0;
    public volatile Integer UPDATED_COUNT = 0;

    private static String PATH = "";

    public static void init() {
        String port = System.getProperty("server.port", "8080");
        //PATH = "C:/tianchi/_" + port + "/";
        PATH = "/usr/local/src/" + port + "/";
        File f = new File(PATH);
        if (!f.exists()) {
            f.mkdir();
        }
    }

    @RequestMapping("/updateWrongTraceId")
    private String updateWrongTraceId(@RequestParam String wrongTradeIdsStr, @RequestParam String port) {

        Map<String, List<String>> result = new ConcurrentHashMap<String, List<String>>();

        try {
            if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
                wrong_8000 = JSON.parseObject(wrongTradeIdsStr, new TypeReference<Map<String, List<String>>>() {
                });
                wrong_return_8000 = JSON.parseObject(wrongTradeIdsStr, new TypeReference<Map<String, List<String>>>() {
                });
                LOGGER.info(String.format("Received Client<8000> Wrong Trade Ids : %s", wrong_8000.size()));
            }
            if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
                wrong_8001 = JSON.parseObject(wrongTradeIdsStr, new TypeReference<Map<String, List<String>>>() {
                });
                wrong_return_8001 = JSON.parseObject(wrongTradeIdsStr, new TypeReference<Map<String, List<String>>>() {
                });
                LOGGER.info(String.format("Received Client<8001> Wrong Trade Ids : %s", wrong_8001.size()));
            }
            UPDATED_COUNT++;
            while (true) {
                try {
                    Thread.sleep(10);
                    if (UPDATED_COUNT == 2) {
                        break;
                    }
                } catch (Exception e) {
                }
            }
            //if 8000, remove all 8000 trace id in 8001
            if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
                Set<String> keys = wrong_8000.keySet();
                for (String traceId : keys) {
                    wrong_return_8001.remove(traceId);
                }
                result = wrong_return_8001;
                LOGGER.info(String.format("Return to Client<8000> Wrong Trade Ids : %s", result.size()));
            }

            if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
                Set<String> keys = wrong_8001.keySet();
                for (String traceId : keys) {
                    wrong_return_8000.remove(traceId);
                }
                result = wrong_return_8000;
                LOGGER.info(String.format("Return to Client<8000> Wrong Trade Ids : %s", result.size()));
            }
        } catch (Exception e) {
            LOGGER.error("updateWrongTraceId failed", e.getMessage());
        }
        return JSON.toJSONString(result);
    }

    @PostMapping("/sendResultFile")
    public String sendResultFile(@RequestParam MultipartFile file) {
        try {
            File f = new File(PATH + file.getOriginalFilename());
            LOGGER.info("Get MultipartFile : " + file.getName());
            file.transferTo(f);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("sendResultFile Exception : ", e);
        }
        return "suc";
    }

    @RequestMapping("/setWrongTraceMap")
    public String setWrongTraceMap(@RequestParam String wrongTraceMap, @RequestParam Integer port) {
        Map<String, Set<String>> processMap = JSON.parseObject(wrongTraceMap, new TypeReference<Map<String, Set<String>>>() {
        });
        if (processMap == null || CollectionUtils.isEmpty(processMap)) {
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

            //call command zip file
            String unZipCommand = String.format("cd %s && tar -zxvf result_%s.tar.gz", PATH, Constants.CLIENT_PROCESS_PORT1);
            String[] commandArr = {"/bin/sh", "-c", unZipCommand};
            LOGGER.info("Calling command: " + unZipCommand);
            Process p = Runtime.getRuntime().exec(commandArr);
            p.waitFor();

            String unZipCommand2 = String.format("cd %s && tar -zxvf result_%s.tar.gz", PATH, Constants.CLIENT_PROCESS_PORT2);
            String[] commandArr2 = {"/bin/sh", "-c", unZipCommand2};
            LOGGER.info("Calling command: " + unZipCommand2);
            Process p2 = Runtime.getRuntime().exec(commandArr2);
            p2.waitFor();


            //call command cat file
            String catCommand = String.format("cd %s && cat result_*.data > all_result.data", PATH);
            String[] commandArr3 = {"/bin/sh", "-c", catCommand};
            LOGGER.info("Calling command: " + catCommand);

            Process p3 = Runtime.getRuntime().exec(commandArr3);
            p3.waitFor();

            File f = new File(PATH + "all_result.data");
            InputStream input = new FileInputStream(f);
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            String line;
            Set<String> set = null;

            while ((line = bf.readLine()) != null) {
                String[] cols = line.split("\\|");
                String traceId = cols[0];
                set = resultMap.get(traceId);
                if (set == null) {
                    set = new HashSet<String>();
                    set.add(line);
                    resultMap.put(traceId, set);
                } else {
                    set.add(line);
                }
            }
            bf.close();
            input.close();

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











