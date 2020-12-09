package com.alibaba.tailbase.clientprocess;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    public static String PATH = "";
    private static final int CURL_SIZE = 200;
    private static final int CURL_BATCH = 30;
    private static List<CurlThread> curlThreads = new ArrayList<CurlThread>();
    private static List<ScanThread> scanThreads = new ArrayList<ScanThread>();

    public static Set<String> wrongTraceIds = Collections.synchronizedSet(new HashSet<String>());
    public static Set<String> allSpans = Collections.synchronizedSet(new HashSet<String>());

    public final static long BYTES_OF_20000_ROWS = 6000000;

    public static void init() {

    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        String port = System.getProperty("server.port", "8080");
        PATH = "/usr/local/src/" + port + "/";
        //PATH = "C:/tianchi/_" + port + "/";
        String url = getUrl();
        LOGGER.info("data url:" + url);
        try {
            File f = new File(PATH);
            if (!f.exists()) {
                f.mkdir();
            }

            ExecutorService pool = Executors.newCachedThreadPool();
            CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(pool);
            URL u = new URL(url);
            HttpURLConnection httpConnection = (HttpURLConnection) u.openConnection(Proxy.NO_PROXY);
            long size = httpConnection.getContentLengthLong();
            long partSize = size / CURL_SIZE;

            long start = 0;
            long end = 0;
            LOGGER.info("URL:" + url + ", Content Length Long: " + size);

            for (int i = 0; i < CURL_SIZE; i++) {
                if (i + 1 == CURL_SIZE) {
                    end = size;
                } else {
                    end = partSize + start;
                }
                if(end < size) {
                    // to calc each start is a new line.
                    InputStream is = getInputStream(url, end, partSize);
                    BufferedInputStream bis = new BufferedInputStream(is);
                    BufferedReader bf = new BufferedReader(new InputStreamReader(bis), 128 * 1024 * 1024);
                    String line = bf.readLine();
                    end = end + line.getBytes().length;
                    is.close();
                    bis.close();
                    bf.close();
                }
                curlThreads.add(new CurlThread(start, end, PATH, url, size));
                start = end;
            }
            int count = 0;

            for (Iterator<CurlThread> it = curlThreads.iterator(); it.hasNext();) {
                CurlThread ct = (CurlThread) it.next();
                if(count < CURL_BATCH){
                    completionService.submit(ct);
                    it.remove();
                    count++;
                }else if(count == CURL_BATCH){
                    completionService.take();
                    it = curlThreads.iterator();
                    count--;
                }
            }
            //wait the remaining workers complete.
            for (int i = 0; i < CURL_BATCH; i++) {
                completionService.take();
            }
            pool.shutdown();
            LOGGER.info("End getting wrong trace ids.");

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("fail to download and split wrong trace ids.", e);
        }

        try {
            //File folder = new File(PATH);
            //List<String> filenames = Arrays.asList(folder.list());

            //using for grep all spans before send to blue block;
            Map<String, List<String>> rangeWithTraceIds = new HashMap<String, List<String>>();
            //using for integrate trade ids in blue block;
            Map<String, List<String>> tradeIdWithRanges = new HashMap<String, List<String>>();


            for (String filename : wrongTraceIds) {
                //for testing
                if(StringUtils.endsWithIgnoreCase(filename, ".data") || StringUtils.endsWithIgnoreCase(filename, ".tar.gz") ) {
                    continue;
                }
                String[] ss = StringUtils.split(filename, "_");
                String wrongTraceId = ss[0];
                String range = ss[1];
                List<String> wrongTraceIds = rangeWithTraceIds.get(range);
                if(wrongTraceIds == null) {
                    wrongTraceIds = new ArrayList<String>();
                    wrongTraceIds.add(wrongTraceId);
                    rangeWithTraceIds.put(range, wrongTraceIds);
                } else {
                    wrongTraceIds.add(wrongTraceId);
                }
                List<String> ranges = tradeIdWithRanges.get(wrongTraceId);
                if(ranges == null) {
                    ranges = new ArrayList<String>();
                    ranges.add(range);
                    tradeIdWithRanges.put(wrongTraceId, ranges);
                }else {
                    ranges.add(range);
                }
            }

            LOGGER.info("Got Range with Trace Ids: " + rangeWithTraceIds.size());
            LOGGER.info("Got Trace with Ranges: " + tradeIdWithRanges.size());

            {
                ExecutorService pool2 =Executors.newCachedThreadPool();
                CompletionService<Boolean> completionService2 = new ExecutorCompletionService<Boolean>(pool2);

                Iterator<Entry<String, List<String>>>  itt = rangeWithTraceIds.entrySet().iterator();

                while (itt.hasNext()) {
                    Entry<String, List<String>> e = itt.next();
                    scanThreads.add(new ScanThread(e, PATH, url));
                }

                int count = 0;
                for (Iterator<ScanThread> it = scanThreads.iterator(); it.hasNext();) {
                    ScanThread st = (ScanThread) it.next();
                    if(count < CURL_BATCH){
                        completionService2.submit(st);
                        it.remove();
                        count++;
                    }else if(count == CURL_BATCH){
                        completionService2.take();
                        it = scanThreads.iterator();
                        count--;
                    }
                }
                //wait the remaining workers complete.
                for (int k = 0; k < CURL_BATCH; k++) {
                    completionService2.take();
                }
                pool2.shutdown();
            }

            Map<String, List<String>> remainingTradeIdWithRange  = updateWrongTraceId(tradeIdWithRanges, port);
            Map<String, List<String>> remainingRangeWithTraceIds = new HashMap<String, List<String>>();
            //convert to remainingRangeWithTraceId
            {
                Iterator<Entry<String, List<String>>> it = remainingTradeIdWithRange.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, List<String>> e = it	.next();
                    List<String> ranges = e.getValue();
                    for (String range : ranges) {
                        List<String> traceIds = remainingRangeWithTraceIds.get(range);
                        if(traceIds == null) {
                            traceIds = new ArrayList<String>();
                            traceIds.add(e.getKey());
                            remainingRangeWithTraceIds.put(range, traceIds);
                        } else {
                            traceIds.add(e.getKey());
                        }
                    }
                }
            }

            LOGGER.info("Got Remaining Range with Trace Ids: " + remainingRangeWithTraceIds.size());
            LOGGER.info("Got Remaining Trace with Ranges: " + remainingTradeIdWithRange.size());

            {
                ExecutorService pool3 =Executors.newCachedThreadPool();
                CompletionService<Boolean> completionService3 = new ExecutorCompletionService<Boolean>(pool3);

                Iterator<Entry<String, List<String>>>  ittt = remainingRangeWithTraceIds.entrySet().iterator();

                while (ittt.hasNext()) {
                    Entry<String, List<String>> e = ittt.next();
                    scanThreads.add(new ScanThread(e, PATH, url));
                }

                int count = 0;
                for (Iterator<ScanThread> it = scanThreads.iterator(); it.hasNext();) {
                    ScanThread st = (ScanThread) it.next();
                    if(count < CURL_BATCH){
                        completionService3.submit(st);
                        it.remove();
                        count++;
                    }else if(count == CURL_BATCH){
                        completionService3.take();
                        it = scanThreads.iterator();
                        count--;
                    }
                }
                //wait the remaining workers complete.
                for (int k = 0; k < CURL_BATCH; k++) {
                    completionService3.take();
                }
                pool3.shutdown();
            }

            File f = new File(PATH + "result_" + port + ".data");

            FileUtils.writeLines(f, "UTF-8", allSpans);

            //call command zip file
            String zipCommand = String.format("cd %s && tar -zcvf result_%s.tar.gz result_%s.data", PATH, port, port);
            String[] commandArr2 = {"/bin/sh", "-c", zipCommand};
            LOGGER.info("Calling command: " + zipCommand);
            Process p2 = Runtime.getRuntime().exec(commandArr2);
            p2.waitFor();

            sendResultFile();
            callFinish();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("fail to process data", e);
        }
    }

    private static InputStream getInputStream(String path, long skip, long partSize) throws Exception {
        URL url = new URL(path);
        HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
        httpConnection.setRequestProperty("Range", "bytes=" + skip + "-" + (skip + partSize));
        httpConnection.setRequestProperty("Connection","Keep-Alive");
        return httpConnection.getInputStream();
    }

    private Map<String, List<String>> updateWrongTraceId(Map<String, List<String>> wrongTradeIds, String port) {

        Map<String, List<String>> remaining = new HashMap<String, List<String>>();

        try {

            String json = JSON.toJSONString(wrongTradeIds);
            RequestBody body = new FormBody.Builder()
                    .add("wrongTradeIdsStr", json)
                    .add("port", port)
                    .build();
            Request request = new Request.Builder().url("http://localhost:8002/updateWrongTraceId").post(body).build();
            Response response = Utils.callHttp(request);
            remaining = JSON.parseObject(response.body().string(), new TypeReference<Map<String, List<String>>>(){});
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to updateWrongTraceId, wrongTradeIds:" + wrongTradeIds, e.getMessage());
        }
        return remaining;
    }

    private void sendResultFile() {
        try {
            String port = System.getProperty("server.port", "8080");
            File zipFile = new File(PATH + "result_" + port + ".tar.gz");
            if(zipFile.exists()){
                RequestBody body = new MultipartBody.Builder()
                        .addFormDataPart("file", zipFile.getName(), RequestBody.create(MediaType.parse("media/type"), zipFile))
                        .build();
                Request request = new Request.Builder().url("http://localhost:8002/sendResultFile").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            }else {
                LOGGER.error("File not exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to uploadResultFile", e);
        }
    }

    // notify backend process when client process has finished.
    private void callFinish() {
        try {
            String port = System.getProperty("server.port", "8080");
            RequestBody body = new FormBody.Builder().add("port", port).build();
            Request request = new Request.Builder().url("http://localhost:8002/finish").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to callFinish");
        }
    }

    public static boolean isDev() {
        return Boolean.valueOf(System.getProperty("is.dev", "false"));
    }

    private static String getUrl() {
        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            if (isDev()) {
                return "http://localhost:8080/trace1.data";
            } else {
                return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
            }
        } else if ("8001".equals(port)) {
            if (isDev()) {
                return "http://localhost:8080/trace2.data";
            } else {
                return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
            }
        } else {
            return null;
        }
    }

}










