package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.proto.BackendServiceGrpc;
import com.alibaba.tailbase.proto.setWrongTraceIdReply;
import com.alibaba.tailbase.proto.setWrongTraceIdRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class BackendServiceGrpcImpl extends BackendServiceGrpc.BackendServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(com.alibaba.tailbase.backendprocess.BackendServiceGrpcImpl.class.getName());

    @Override
    public void setWrongTraceId(setWrongTraceIdRequest req, StreamObserver<setWrongTraceIdReply> responseObserver) {
        setWrongTraceIdReply reply = setWrongTraceIdReply.newBuilder().setMessage("suc").build();

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

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
