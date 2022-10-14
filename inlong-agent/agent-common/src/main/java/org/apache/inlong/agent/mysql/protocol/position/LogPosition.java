package org.apache.inlong.agent.mysql.protocol.position;


import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

/**
 * 基于mysql/oracle log位置标示
 * 
 * @author jianghang 2012-6-21 上午10:52:41
 * @version 1.0.0
 */
public class LogPosition extends Position implements Comparable<LogPosition>{

    private static final long serialVersionUID = 3875012010277005819L;
    private LogIdentity       identity;
    private EntryPosition     position;
    private boolean bAcked = false;
    private long genTimeStample;
    private int sendIndex = 0;
    private long pkgIndex = 0;
    private String parseThreadName;

    public LogPosition(){
    	genTimeStample = System.currentTimeMillis();
    }
    
    public LogPosition(LogPosition other){
    	this.identity = new LogIdentity(other.identity);
    	this.position = new EntryPosition(other.position);
    	this.sendIndex = other.sendIndex;
    	this.pkgIndex = other.pkgIndex;
        this.parseThreadName = other.parseThreadName;
    }
    
    public LogPosition(JSONObject obj){
    	this.identity = new LogIdentity(obj.getJSONObject("logIdentity"));
    	this.position = new EntryPosition(obj.getJSONObject("entryPosition"));
    }

    public LogIdentity getIdentity() {
        return identity;
    }

    public void setIdentity(LogIdentity identity) {
        this.identity = identity;
    }

    public EntryPosition getPosition() {
        return position;
    }

    public void setPosition(EntryPosition position) {
        this.position = position;
    }
    
    public void ackePosiiton(){
    	bAcked = true;
    }
    
    public boolean bAcked(){
    	return bAcked;
    }
    
    public long getGenTimeStample(){
    	return genTimeStample;
    }

    public int getSendIndex() {
        return sendIndex;
    }

    public void setSendIndex(int sendIndex) {
        this.sendIndex = sendIndex;
    }

    public long getPkgIndex() {
        return pkgIndex;
    }

    public void setPkgIndex(long pkgIndex) {
        this.pkgIndex = pkgIndex;
    }

    public JSONObject getJsonObj(){
    	JSONObject obj = new JSONObject();
    	obj.put("logIdentity", this.identity.getJsonObj());
    	obj.put("entryPosition", this.position.getJsonObj());
    	obj.put("sendIndex", this.getSendIndex());
        obj.put("pkgIndex", this.getPkgIndex());
        obj.put("parseJobName", this.getParseThreadName());
        return obj;
    }
    
    @Override
    public String toString(){
    	JSONObject obj = new JSONObject();
    	obj.put("logIdentity", this.identity.getJsonObj());
    	obj.put("entryPosition", this.position.getJsonObj());
        obj.put("sendIndex", this.getSendIndex());
        obj.put("pkgIndex", this.getPkgIndex());
        obj.put("parseJobName", this.getParseThreadName());
        return obj.toJSONString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identity == null) ? 0 : identity.hashCode());
        result = prime * result + ((position == null) ? 0 : position.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof LogPosition)) {
            return false;
        }
        LogPosition other = (LogPosition) obj;
        if (identity == null) {
            if (other.identity != null) {
                return false;
            }
        } else if (!identity.equals(other.identity)) {
            return false;
        }
        if (position == null) {
            if (other.position != null) {
                return false;
            }
        } else if (!position.equals(other.position)) {
            return false;
        }
        if (sendIndex != other.sendIndex) {
            return false;
        }
        if (pkgIndex != other.pkgIndex) {
            return false;
        }
        if (parseThreadName == null) {
            if (other.parseThreadName != null) {
                return false;
            }
        } else if (!parseThreadName.equals(other.parseThreadName)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LogPosition otherPos) {
        long cmp = 0;
        if (identity != null) {
            cmp = identity.compareTo(otherPos.identity);
        }
        if (cmp == 0 && position != null) {
            cmp = position.compareTo(otherPos.position);
        }
        if (cmp == 0) {
            cmp = sendIndex - otherPos.getSendIndex();
        }
        if (cmp == 0) {
            cmp = pkgIndex - otherPos.getPkgIndex();
        }
        return (int) cmp;
    }


    public String getParseThreadName() {
        return parseThreadName;
    }

    public void setParseThreadName(String parseThreadName) {
        this.parseThreadName = parseThreadName;
    }
}
