package com.hzwz.tailbase.backend;

import com.hzwz.tailbase.Constants;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description 暂存错误的traceId，用于向client拉取具体数据
 * @date 2020/5/22 23:21
 **/
public class TraceIdBucket {
    private final AtomicInteger pos = new AtomicInteger(-1);
    private final AtomicInteger processCount = new AtomicInteger(0);
    // 一个bucket中20条traceId
    private Set<String> traceIdSet = new HashSet<>(Constants.BUCKET_ERR_TRACE_COUNT);

    public int getPos() {
        return pos.get();
    }

    public void setPos(int pos) {
        this.pos.set(pos);
    }

    public int getProcessCount() {
        return processCount.get();
    }

    public Set<String> getTraceIdSet() {
        return traceIdSet;
    }

    public int addProcessCount() {
        return processCount.addAndGet(1);
    }

    public void reset() {
        pos.set(-1);
        processCount.set(0);
        traceIdSet.clear();
    }
}
