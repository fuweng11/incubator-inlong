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

package org.apache.inlong.agent.metrics.dbsync;

import org.apache.inlong.agent.mysql.protocol.position.LogPosition;

import lombok.Data;

@Data
public class StatisticInfo {

    private LogPosition latestLogPosition;
    private String key;
    private String oldKey;
    private String groupID;
    private String streamID;
    private String topic;
    private long timestamp;
    private String dbJobId = "";

    public StatisticInfo(String groupID, String streamID, long timestamp,
            LogPosition logPosition, String dbJobId, String key) {
        this.groupID = groupID;
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.latestLogPosition = logPosition;
        this.dbJobId = dbJobId;
        this.key = key;
    }

    public LogPosition getLatestLogPosition() {
        return latestLogPosition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupID() {
        return groupID;
    }

    public String getStreamID() {
        return streamID;
    }
}
