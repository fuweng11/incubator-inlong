package org.apache.inlong.agent.mysql.protocol.position;

import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

import java.util.Objects;

/**
 * 数据库对象的唯一标示
 * 
 * @author lyndldeng 2016-2-25
 * @version 1.0.0
 */
public class EntryPosition extends TimePosition implements Comparable<EntryPosition>{

    public static final int EVENTIDENTITY_SEGMENT = 3;
    public static final char EVENTIDENTITY_SPLIT = (char) 5;
    private static final long serialVersionUID = 81432665066427482L;
    private boolean included = false;
    private String journalName;
    private Long position;
    private Long serverId;
//    private String gtid;

    public EntryPosition(){
        super(null);
    }

    public EntryPosition(Long timestamp){
        this(null, null, timestamp);
    }

    public EntryPosition(String journalName, Long position){
        this(journalName, position, null);
    }

    public EntryPosition(String journalName, Long position, Long timestamp){
        super(timestamp);
        this.journalName = journalName;
        this.position = position;
    }

    public EntryPosition(String journalName, Long position, Long timestamp, Long serverId){
        this(journalName, position, timestamp);
        this.serverId = serverId;
    }
    
    /*add deep copy*/
    public EntryPosition(EntryPosition other){
    	super(other.timestamp);
    	this.journalName = other.journalName;
    	this.position = other.position;
//    	this.gtid =  other.gtid;
        this.serverId = other.serverId;
    	this.included = other.included;
    }
    
    public EntryPosition(JSONObject obj) {
    	super(obj.getLong("timestamp"));
    	this.journalName = obj.getString("journalName");
    	this.position = obj.getLong("position");
    	this.included = obj.getBoolean("included");
    	this.serverId = obj.getLong("serverId");
    }


    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public boolean isIncluded() {
        return included;
    }

    public void setIncluded(boolean included) {
        this.included = included;
    }

//    public String getGtid() {
//        return gtid;
//    }
//
//    public void setGtid(String gtid) {
//        this.gtid = gtid;
//    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

//    public boolean hasGtid() {
//        return StringUtils.isNotBlank(gtid);
//    }

    public JSONObject getJsonObj(){
    	JSONObject obj = new JSONObject();
    	obj.put("journalName", this.journalName);
    	obj.put("position", this.position);
    	obj.put("included", this.included);
//    	if (hasGtid()) {
//            obj.put("gtid", this.gtid);
//        }
    	obj.put("timestamp", this.timestamp);
        obj.put("serverId", this.serverId);
    	return obj;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((journalName == null) ? 0 : journalName.hashCode());
        result = prime * result + ((position == null) ? 0 : position.hashCode());
        // please note when use equals
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof EntryPosition)) {
            return false;
        }
        EntryPosition other = (EntryPosition) obj;
        if (journalName == null) {
            if (other.journalName != null) {
                return false;
            }
        } else if (!journalName.equals(other.journalName)) {
            return false;
        }
        if (position == null) {
            if (other.position != null) {
                return false;
            }
        } else if (!position.equals(other.position)) {
            return false;
        }
        // please note when use equals
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(EntryPosition o) {
        if (timestamp != null && o.timestamp != null) {
            if (!Objects.equals(timestamp, o.timestamp)) {
                return (int)(timestamp - o.timestamp);
            }
        }

        final int val = journalName.compareTo(o.journalName);
        if (val == 0) {
            return (int)(position - o.position);
        }
        return val;
    }

}
