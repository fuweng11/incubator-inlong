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

package org.apache.inlong.agent.entites;

import org.apache.inlong.common.pojo.agent.dbsync.DbSyncAddFieldRequest.FieldObject;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FieldChangedEntry implements Comparable<FieldChangedEntry> {

    private String groupId;
    private Integer taskId;
    private String alterQueryString;
    private long lastReportTime = 0L;
    private int sendTimes = 0;
    private List<FieldObject> fieldChangedList;

    public FieldChangedEntry(String groupId, Integer taskId, String alterQueryString) {
        this.groupId = groupId;
        this.taskId = taskId;
        this.alterQueryString = alterQueryString;
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
