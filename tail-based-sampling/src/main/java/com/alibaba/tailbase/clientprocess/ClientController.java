package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.backendprocess.TraceIdBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.alibaba.tailbase.clientprocess.ClientProcessData.batchQueue;


@RestController
public class ClientController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());


    @RequestMapping("/setWrongTraceIdBatch")
    public String setWrongTraceId(@RequestParam String batch) {
        TraceIdBatch traceIdBatch = JSON.parseObject(batch, new TypeReference<TraceIdBatch>() {
        });
        batchQueue.offer(traceIdBatch);
        return "suc";
    }
}
