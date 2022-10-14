package org.apache.inlong.agent.core.ha;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class JobHaInfo {

    @JSONField(ordinal = 1)
    private String syncId;
    /*
     * is or not connect to zk
     */
    @JSONField(ordinal = 2)
    private volatile String syncPosition;

    @JSONField(ordinal = 3)
    private volatile boolean isZkHealth;

    @JSONField(ordinal = 4)
    private String jobName;
    /*
     * taskId ,taskConf
     */
    @JSONField(ordinal = 5)
    private ConcurrentHashMap<Integer, DbSyncTaskInfo> taskConfMap = new ConcurrentHashMap<>();


    public DbSyncTaskInfo addTaskConf(DbSyncTaskInfo taskConf) {
        return taskConfMap.put(taskConf.getId(), taskConf);
    }

    public DbSyncTaskInfo deleteTaskConf(Integer taskId) {
        return taskConfMap.remove(taskId);
    }

    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobHaInfo jobHaInfo = (JobHaInfo) o;
        return Objects.equals(syncId, jobHaInfo.syncId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(syncId);
    }
}
