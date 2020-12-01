package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.proto.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Component
public class BackendServiceGrpcImpl extends BackendServiceGrpc.BackendServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(com.alibaba.tailbase.backendprocess.BackendServiceGrpcImpl.class.getName());

    @Override
    public void setWrongTraceId(setWrongTraceIdRequest req, StreamObserver<setWrongTraceIdReply> responseObserver) {
        int batchPos = req.getBatchPos();
        int pos = batchPos % BackendController.BATCH_COUNT;
        List<String> traceIdList = JSON.parseObject(req.getTraceIdListJson(), new TypeReference<List<String>>() {
        });
        LOGGER.info(String.format("setWrongTraceId had called, batchPos:%d, traceIdList:%s", batchPos, req.getTraceIdListJson()));
        TraceIdBatch traceIdBatch = BackendController.TRACEID_BATCH_LIST.get(pos);
        if (traceIdBatch.getBatchPos() != 0 && traceIdBatch.getBatchPos() != batchPos) {
            LOGGER.warn("overwrite traceId batch when call setWrongTraceId");
        }

        if (traceIdList != null && traceIdList.size() > 0) {
            traceIdBatch.setBatchPos(batchPos);
            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
            traceIdBatch.getTraceIdList().addAll(traceIdList);
        }

        setWrongTraceIdReply reply = setWrongTraceIdReply.newBuilder().setMessage("suc").build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void sendWrongTracing(sendWrongTracingRequest req, StreamObserver<sendWrongTracingReply> responseObserver) {

        Map<String, List<String>> processMap = JSON.parseObject(req.getWrongTraceMap(),
                new TypeReference<Map<String, List<String>>>() {
                });

        String bathPos = req.getBatchPos();
        List<Map<String, List<String>>> values = CheckSumService.TRACE_CHECKSUM_MAP_RAW.get(bathPos);
        if (values == null) {
            values = new ArrayList<>(2);
            CheckSumService.TRACE_CHECKSUM_MAP_RAW.put(bathPos, values);
        }

        if (processMap != null) {
            values.add(processMap);
        } else {
            values.add(new HashMap<>(1));
        }

        sendWrongTracingReply reply = sendWrongTracingReply.newBuilder().setMessage("suc").build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();

    }
}
