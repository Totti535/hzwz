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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    private static String PATH = "";
    public static final int CURL_SIZE = 50;
    public static final int DOWNLOAD_BATCH = 50;
    public static final int SCAN_SIZE = 50;
    public static final int SCAN_BATCH = 10;
    public static final List<long[]> indexes = new ArrayList<long[]>();

    private static List<CurlThread> curlThreads = new ArrayList<CurlThread>();
    private static List<ScanThread> scanThreads = new ArrayList<ScanThread>();
    private static int GREP_BATCH_SIZE = 1000;

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
            long size = httpConnection.getContentLengthLong()/8;
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
                long[] idx = new long[2];
                idx[0] = start;
                idx[1] = end;
                indexes.add(idx);
                curlThreads.add( new CurlThread(start, end, PATH, url));
                start = end;
            }
            int count = 0;

            for (Iterator<CurlThread> it = curlThreads.iterator(); it.hasNext();) {
                CurlThread ct = (CurlThread) it.next();
                if(count < DOWNLOAD_BATCH){
                    completionService.submit(ct);
                    it.remove();
                    count++;
                }else if(count == DOWNLOAD_BATCH){
                    completionService.take();
                    it = curlThreads.iterator();
                    count--;
                }
            }
            //wait the remaining workers complete.
            for (int i = 0; i < DOWNLOAD_BATCH; i++) {
                completionService.take();
            }
            pool.shutdown();
            LOGGER.info("End getting wrong trace ids.");

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("fail to download and split wrong trace ids.", e);
        }

        try {
            File folder = new File(PATH);
            updateWrongTraceId(Arrays.asList(folder.list()));

            Set<String> allWti = getAllWrongTraceIds();

            int i = 1;

            StringBuffer sb = new StringBuffer();
            List<String> commandList = new ArrayList<String>();

            for (String traceId : allWti) {
                if(i >= GREP_BATCH_SIZE) {
                    sb.append(traceId);
                }else {
                    sb.append(traceId + "|");
                }
                if(i % GREP_BATCH_SIZE == 0) {
                    commandList.add(sb.toString());
                    sb = new StringBuffer();
                    i = 1;
                }
                i++;
            }
            commandList.add(sb.toString());

            ExecutorService pool2 =Executors.newCachedThreadPool();
            CompletionService<Boolean> completionService2 = new ExecutorCompletionService<Boolean>(pool2);

            String url = getUrl();
            for (int j = 0; j < SCAN_SIZE; j++) {
                long[] idx = indexes.get(j);
                long start = idx[0];
                long end = idx[1];
                scanThreads.add(new ScanThread(start, end, PATH, url, commandList, (j+1)));
            }


                int count = 0;
            for (Iterator<ScanThread> it = scanThreads.iterator(); it.hasNext();) {
                ScanThread st = (ScanThread) it.next();
                if(count < SCAN_BATCH){
                    completionService2.submit(st);
                    it.remove();
                    count++;
                }else if(count == SCAN_BATCH){
                    completionService2.take();
                    it = scanThreads.iterator();
                    count--;
                }
            }
            //wait the remaining workers complete.
            for (int k = 0; k < SCAN_BATCH; k++) {
                completionService2.take();
            }
            pool2.shutdown();

//            String port = System.getProperty("server.port", "8080");

//            call command cat file
            String catCommand = String.format("cd %s && cat spans_*.data > result_%s.data", PATH, port);
            String[] commandArr = {"/bin/sh", "-c", catCommand};
            LOGGER.info("Calling command: " + catCommand);
            Process p = Runtime.getRuntime().exec(commandArr);
            p.waitFor();

            //call command zip file
            String zipCommand = String.format("cd %s && zip -q result_%s.zip result_%s.data", PATH, port, port);
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

    private Set<String> getAllWrongTraceIds() {
        Set<String> wrongTraceIds = new HashSet<String>();
        try {
            Request request = new Request.Builder().url("http://localhost:8002/getWrongTraceIds").build();
            Response response= Utils.callHttp(request);
            wrongTraceIds = JSON.parseObject(response.body().string(), new TypeReference<Set<String>>(){});
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("fail to getWrongTraceIds", e);
        }
        return wrongTraceIds;
    }


    private void sendResultFile() {
        try {
            String port = System.getProperty("server.port", "8080");
            File zipFile = new File(PATH + "result_" + port + ".zip");
            if(zipFile.exists()){
                RequestBody body = new MultipartBody.Builder().addFormDataPart("file", zipFile.getName(), RequestBody.create(MediaType.parse("media/type"), zipFile)).build();
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






