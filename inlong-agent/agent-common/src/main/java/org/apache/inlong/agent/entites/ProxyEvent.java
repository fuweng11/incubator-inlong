package org.apache.inlong.agent.entites;

public class ProxyEvent {
    private String message;
    private String bid;
    private String tid;
    private long time;
    private long timeout;

    public ProxyEvent(String message, String bid, String tid, long time, long timeout) {
        this.message = message;
        this.bid = bid;
        this.tid = tid;
        this.time = time;
        this.timeout = timeout;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getBid() {
        return bid;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
