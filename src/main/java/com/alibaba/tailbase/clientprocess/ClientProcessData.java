package com.alibaba.tailbase.clientprocess;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Utils;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    private static String PATH = "";
    public static final int CURL_SIZE = 100;

    public static final int DOWNLOAD_BATCH = 50;
    public static final int PRO_TRACE_ID_BATCH = 100;

    private static List<CurlThread> threads = new ArrayList<CurlThread>();

    public static void init() {


    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {

        String port = System.getProperty("server.port", "8080");
//        if (isDev()) {
//            PATH = "C:\\tianchi\\_" + port + "\\";
//        } else {
        PATH = "/usr/local/src/" + port + "/";
//        }
        try {
            File f = new File(PATH);
            if (!f.exists()) {
                f.mkdir();
            }

            ExecutorService pool = Executors.newCachedThreadPool();
            CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(pool);

            String url = getUrl();
            URL u = new URL(url);
            LOGGER.info("data url:" + url);
            HttpURLConnection httpConnection = (HttpURLConnection) u.openConnection(Proxy.NO_PROXY);
            long size = httpConnection.getContentLengthLong()/4;
            long partSize = size / CURL_SIZE;

            long start = 0;
            long end = 0;
            LOGGER.info("Content Length Long: " + size);

            for (int i = 0; i < CURL_SIZE; i++) {
                if (i + 1 == CURL_SIZE) {
                    end = size;
                } else {
                    end = partSize + start;
                }
                if(end < size) {
                    // to calc each start is a new line.
                    InputStream is = getInputStream(url, end);
                    BufferedInputStream bis = new BufferedInputStream(is);
                    BufferedReader bf = new BufferedReader(new InputStreamReader(bis), 128 * 1024 * 1024);
                    String line = bf.readLine();
                    end = end + line.getBytes().length;
                    is.close();
                    bis.close();
                    bf.close();
                }
                threads.add( new CurlThread(start, end, PATH, url, (i + 1)));
                start = end;
            }
            int count = 0;

            for (Iterator<CurlThread> it = threads.iterator(); it.hasNext();) {
                CurlThread ct = (CurlThread) it.next();
                if(count < DOWNLOAD_BATCH){
                    completionService.submit(ct);
                    it.remove();
                    count++;
                }else if(count == DOWNLOAD_BATCH){
                    completionService.take();
                    it = threads.iterator();
                    count--;
                }
            }
            //wait the remaining workers complete.
            for (int i = 0; i < DOWNLOAD_BATCH; i++) {
                completionService.take();
            }
            pool.shutdown();
            LOGGER.info("End for download and split wrong trace ids.");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("fail to download and split wrong trace ids.", e);
        }

        try {
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File f, String name) {
                    return name.endsWith("_error.data");
                }
            };

            File folder = new File(PATH);

            updateWrongTraceId(Arrays.asList(folder.list(filter)));
            Map<String, List<String>> allWti = getAllWrongTraceIds();

            ExecutorService pool2 =Executors.newCachedThreadPool();
            CompletionService<Boolean> completionService2 = new ExecutorCompletionService<Boolean>(pool2);

            int count2 = 0;
            Set<Entry<String, List<String>>> set = allWti.entrySet();
            for (Iterator<Entry<String, List<String>>> it2 = set.iterator(); it2.hasNext();) {
                Entry<String, List<String>> ct = (Entry<String, List<String>>) it2.next();
                SendSpanThread sst = new SendSpanThread(PATH, ct);
                if(count2 < PRO_TRACE_ID_BATCH){
                    completionService2.submit(sst);
                    it2.remove();
                    count2++;

                }else if(count2 == PRO_TRACE_ID_BATCH){
                    completionService2.take();
                    it2 = set.iterator();
                    count2--;
                }
            }
            pool2.shutdown();
            callFinish();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("fail to process data", e);
        }
    }

    private static InputStream getInputStream(String path, long skip) throws Exception {
        URL url = new URL(path);
        HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
        httpConnection.setRequestProperty("Range", "bytes=" + skip + "-");
        httpConnection.setRequestProperty("Connection","Keep-Alive");
        return httpConnection.getInputStream();
    }

    private void updateWrongTraceId(List<String> wrongTradeIds) {
        try {
            String json = JSON.toJSONString(wrongTradeIds);
            RequestBody body = new FormBody.Builder().add("wrongTradeIdsStr", json).build();
            Request request = new Request.Builder().url("http://localhost:8002/updateWrongTraceId").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to updateWrongTraceId, wrongTradeIds:" + wrongTradeIds, e.getMessage());
        }
    }

    private Map<String, List<String>> getAllWrongTraceIds() {
        Map<String, List<String>> wrongTraceIds = new HashMap<String, List<String>>();
        try {
            Request request = new Request.Builder().url("http://localhost:8002/getWrongTraceIds").build();
            Response response= Utils.callHttp(request);
            wrongTraceIds = JSON.parseObject(response.body().string(), new TypeReference<HashMap<String, List<String>>>(){});
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to getWrongTraceIds", e);
        }
        return wrongTraceIds;
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







