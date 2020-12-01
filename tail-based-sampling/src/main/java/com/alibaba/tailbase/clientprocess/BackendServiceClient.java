package com.alibaba.tailbase.clientprocess;

import com.alibaba.tailbase.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class BackendServiceClient {

    private final ManagedChannel channel;
    private final BackendServiceGrpc.BackendServiceBlockingStub blockingStub;
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendServiceClient.class.getName());

    public BackendServiceClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();

        blockingStub = BackendServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void setWrongTraceId(String json, int batchPo) {
        setWrongTraceIdRequest request = setWrongTraceIdRequest.newBuilder().setTraceIdListJson(json).setBatchPos(batchPo).build();

        setWrongTraceIdReply response;
        try {
            response = blockingStub.setWrongTraceId(request);
        } catch (StatusRuntimeException e) {
            LOGGER.warn("fail to set wrong trace id suc, batchPos: " + batchPo);
            return;
        }
        LOGGER.info("set wrong trace id suc, batchPos: " + batchPo);
    }

    public void sendWrongTracing(String json, String batchPo) {
        sendWrongTracingRequest request = sendWrongTracingRequest.newBuilder().setWrongTraceMap(json).setBatchPos(batchPo).build();

        sendWrongTracingReply response;
        try {
            response = blockingStub.sendWrongTracing(request);
        } catch (StatusRuntimeException e) {
            LOGGER.warn(" fail to send wrong tracing suc, batchPos: " + batchPo);
            return;
        }
        LOGGER.info("send wrong tracing suc, batchPos: " + batchPo);
    }
}