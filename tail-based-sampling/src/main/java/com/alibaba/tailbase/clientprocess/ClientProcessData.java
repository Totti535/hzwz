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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    public static Queue<String> SOURCE_DATA_QUEUE = new ConcurrentLinkedQueue<>();

    public static boolean IS_READ_FINISHED = false;

    private static String port;

    public static void init() {
        port = System.getProperty("server.port");
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {

        try {
            //InputStream input = getSourceDataInputStream();
            //BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            ClientDataHandler.start();
/*
            String line;
            long count = 0;
            while ((line = bf.readLine()) != null) {
                if (SOURCE_DATA_QUEUE.size() <= Constants.BATCH_SIZE * 4) {
                    SOURCE_DATA_QUEUE.offer(line);
                    count++;
                }
            }
            IS_READ_FINISHED = true;
            bf.close();
            input.close();
            LOGGER.info(String.format(" read %s lines.", count));

 */
        } catch (Exception e) {
            LOGGER.warn("fail to read source data", e);
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
