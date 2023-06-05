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

package org.apache.inlong.agent.message;

import org.apache.inlong.agent.mysql.protocol.position.LogPosition;

import java.util.Map;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;

public class DBSyncMessage extends DefaultMessage {

    private LogPosition logPosition;
    /* the binlog time stample is second, use msgId for same time stample */
    private long msgId;
    private String jobName; // namely jobName

    private Long msgTimeStamp;

    public DBSyncMessage(byte[] body, Map<String, String> header, String jobName,
            Long msgTimeStamp) {
        super(body, header);
        this.jobName = jobName;
        this.msgTimeStamp = msgTimeStamp;
    }

    public LogPosition getLogPosition() {
        return logPosition;
    }

    public void setLogPosition(LogPosition logPosition) {
        this.logPosition = logPosition;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Long getMsgTimeStamp() {
        return msgTimeStamp;
    }

    public String getGroupId() {
        return header.getOrDefault(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
    }

    public String getStreamId() {
        return header.getOrDefault(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
    }
}
