package com.alibaba.tailbase;

public class Entry {
    private String traceId;
    private String span;
    private Boolean isWrong;

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpan() {
        return span;
    }

    public void setSpan(String span) {
        this.span = span;
    }

    public Boolean getIsWrong() {
        return isWrong;
    }

    public void setIsWrong(Boolean isWrong) {
        this.isWrong = isWrong;
    }
}
