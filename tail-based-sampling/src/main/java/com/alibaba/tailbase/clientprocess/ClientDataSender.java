package com.alibaba.tailbase.clientprocess;

import com.alibaba.tailbase.backendprocess.TraceIdBatch;

public class ClientDataSender implements Runnable {

    public static void start() {
        Thread t = new Thread(new ClientDataSender(), "ClientDataSenderThread");
        t.start();
    }

    @Override
    public void run() {

        while (true) {

            try {
                if (!ClientProcessData.batchQueue.isEmpty()) {
                    TraceIdBatch traceIdBatch = ClientProcessData.batchQueue.poll();
                    if (traceIdBatch != null) {
                        ClientProcessData.sendWrongTracing(traceIdBatch.getTraceIdList(), traceIdBatch.getBatchPos());
                    }
                }
            }catch (Exception ex) {

            }
        }
    }
}
