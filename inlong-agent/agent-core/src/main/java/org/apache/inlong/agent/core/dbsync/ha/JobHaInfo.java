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

package org.apache.inlong.agent.core.dbsync.ha;

import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class JobHaInfo {

    @JSONField(ordinal = 1)
    private String dbJobId;
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
        return Objects.equals(dbJobId, jobHaInfo.dbJobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbJobId);
    }
}
