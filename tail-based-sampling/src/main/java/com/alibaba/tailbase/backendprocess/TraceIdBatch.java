package com.alibaba.tailbase.backendprocess;

import com.alibaba.tailbase.Constants;

import java.util.*;


public class TraceIdBatch {
    private Set<Integer> ports = new HashSet<>(Constants.PROCESS_COUNT);
    private List<String> traceIdList = new ArrayList<>(Constants.BATCH_SIZE / 100);

    public List<String> getTraceIdList() {
        return traceIdList;
    }

    public Set<Integer> getPorts() {
        return this.ports;
    }

    public boolean isReady() {
       return this.ports.containsAll(Arrays.asList(Constants.CLIENT_DATA_PORTS));
    }

}
