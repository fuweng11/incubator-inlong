/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.conf;

import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class MysqlTableConf {

    public static final String INST_NAME = "inst.name";
    public static final String DATABASE_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";
    public static final String TASK_ID = "task.id";
    public static final String TASK_STREAMID = "task.streamId";
    public static final String TASK_GROUPID = "task.groupId";
    public static final String TASK_SKIP_DELETE = "task.skip_delete";
    public static final String TASK_STATUS = "task.status";
    public static final String TASK_CHARSET_NAME = "task.charset.name";

    private String jobName;
    private String dbName;
    private String tbName;
    private ArrayList<String> fields;
    private Integer taskId;
    private JobStat.TaskStat status;

    private String groupId;
    private String streamId;
    private boolean skipDelete = false;
    private Charset charset;

    public MysqlTableConf(String jobName, String databaseName, String tableName, String groupId, String streamId,
            Integer taskId, Charset charset, boolean skipDelete) {
        this.jobName = jobName;
        this.dbName = databaseName;
        this.tbName = tableName;
        this.groupId = groupId;
        this.streamId = streamId;
        this.taskId = taskId;
        this.skipDelete = skipDelete;
        this.charset = charset;
        this.fields = new ArrayList<>();
    }

    public String getDataBaseName() {
        return dbName;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getJobName() {
        return jobName;
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
        sb.append("In instance : ").append(jobName)
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
        obj.put(INST_NAME, this.jobName);
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
