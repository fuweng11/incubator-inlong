package org.apache.inlong.agent.entites;

import org.apache.inlong.agent.utils.JsonUtils.JSONArray;

public class FieldChangedEntry implements Comparable<FieldChangedEntry>{

    private String groupId;
    private String taskId;
    private String alterQueryString;
    private long lastReportTime = 0L;
    private int sendTimes = 0;
    private JSONArray jsonArray;

    public FieldChangedEntry(String groupId, String taskId, String alterQueryString) {
        this.groupId = groupId;
        this.taskId = taskId;
        this.alterQueryString = alterQueryString;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getSendTimes() {
        return sendTimes;
    }

    public String getAlterQueryString() {
        return alterQueryString;
    }

    public long getLastReportTime() {
        return lastReportTime;
    }

    public void updateLastReportTime(long lastReportTime) {
        this.lastReportTime = lastReportTime;
    }

    public void setJsonArray(JSONArray jsonArray) {
        this.jsonArray = jsonArray;
    }

    public JSONArray getJsonArray() {
        return jsonArray;
    }

    public void addSendTimes() {
        sendTimes++;
    }

    @Override
    public int compareTo(FieldChangedEntry o) {
        if (!groupId.equals(o.getGroupId())) {
            return groupId.compareTo(o.getGroupId());
        }

        if (!taskId.equals(o.getTaskId())) {
            return taskId.compareTo(o.getTaskId());
        }

        if (!alterQueryString.equals(o.getAlterQueryString())) {
            return alterQueryString.compareTo(o.getAlterQueryString());
        }
        return 0;
    }
}
