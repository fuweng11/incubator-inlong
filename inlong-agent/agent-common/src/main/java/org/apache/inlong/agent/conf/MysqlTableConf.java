package org.apache.inlong.agent.conf;

import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

import java.nio.charset.Charset;
import java.util.ArrayList;

//TODO: directly use DBSyncTaskConf
public class MysqlTableConf {

    public static final String INST_NAME = "inst.name";
    public static final String DATABASE_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";
    public static final String TOPIC = "topic";
    public static final String TASK_ID = "task.id";
    public static final String TASK_STREAMID = "task.streamId";
    public static final String TASK_GROUPID = "task.groupId";
    public static final String TASK_SKIP_DELETE = "task.skip_delete";
    public static final String TASK_STATUS = "task.status";
    public static final String TASK_CHARSET_NAME = "task.charset.name";

    private String instName;
    private String dbName;
    private String tbName;
    private ArrayList<String> fields;
    private Integer taskId;
    private JobStat.TaskStat status;

    private String groupId;
    private String streamId;
    private boolean skipDelete = false;
    private Charset charset;


    public MysqlTableConf(String instName, String databaseName, String tableName, String groupId, String streamId,
            Integer taskId, Charset charset, boolean skipDelete) {
        this.instName = instName;
        this.dbName = databaseName;
        this.tbName = tableName;
        this.groupId = groupId;
        this.streamId = streamId;
        this.taskId = taskId;
        this.skipDelete = skipDelete;
        this.charset = charset;
        this.fields = new ArrayList<>();
    }

    public MysqlTableConf(JSONObject obj, Charset charset) {

        this.instName = obj.getString(INST_NAME);
        this.dbName = obj.getString(DATABASE_NAME);
        this.tbName = obj.getString(TABLE_NAME);
        this.taskId = obj.getInteger(TASK_ID);
        this.streamId = obj.getString(TASK_STREAMID);
        if (obj.containsKey(TASK_SKIP_DELETE)) {
            this.skipDelete = obj.getBoolean(TASK_SKIP_DELETE);
        }
        this.status = JobStat.TaskStat.valueOf(obj.getString(TASK_STATUS));
        this.charset = charset;
        if (obj.containsKey(TASK_CHARSET_NAME)) {
            //replace if there have charset name, else use jobconf charset
            this.charset = Charset.forName(obj.getString(TASK_CHARSET_NAME));
        }
    }

    public String getDataBaseName() {
        return dbName;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTableName() {
        return tbName;
    }

    public String getStreamId() {
        return streamId;
    }

    public boolean isSkipDelete() {
        return skipDelete;
    }

    public void setSkipDelete(boolean skipDelete) {
        this.skipDelete = skipDelete;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("In instance : ").append(instName)
                .append(" taskId : ").append(taskId)
                .append(" Database : ").append(dbName)
                .append(" Table : ").append(tbName).append("\n");
        sb.append("\tFeilds :");
        for (String f : fields) {
            sb.append(f).append(" , ");
        }
        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    public JSONObject getJsonObj() {
        JSONObject obj = new JSONObject();
        obj.put(INST_NAME, this.instName);
        obj.put(DATABASE_NAME, this.dbName);
        obj.put(TABLE_NAME, this.tbName);
        obj.put(TASK_GROUPID, this.groupId);
        obj.put(TASK_ID, this.taskId);
        obj.put(TASK_STREAMID, this.streamId);
        obj.put(TASK_SKIP_DELETE, this.skipDelete);
        obj.put(TASK_STATUS, this.status.name());
        obj.put(TASK_CHARSET_NAME, this.charset.name());
        return obj;
    }

    public void updateJobStatus(JobStat.TaskStat newStat) {
        this.status = newStat;
    }

    public JobStat.TaskStat getStatus() {
        return status;
    }

    public Integer getTaskId() {
        return this.taskId;
    }

}
