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
import org.apache.inlong.agent.plugin.Message;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static org.apache.inlong.agent.constant.CommonConstants.*;

/**
 * Bus message with body, header, inlongGroupId and inlongStreamId.
 */
public class ProxyMessage implements Message {

    private static final String DEFAULT_INLONG_STREAM_ID = "__";

    private final byte[] body;
    private final Map<String, String> header;
    private final String inlongGroupId;
    private final String inlongStreamId;
    // determine the group key when making batch
    private final String batchKey;
    private final String dataKey;

    private final String dateKey;

    // dbsync logposition
    private LogPosition logPosition;
    private long msgId;
    private String taskID;

    public ProxyMessage(byte[] body, Map<String, String> header) {
        this.body = body;
        this.header = header;
        this.inlongGroupId = header.get(PROXY_KEY_GROUP_ID);
        this.inlongStreamId = header.getOrDefault(PROXY_KEY_STREAM_ID, DEFAULT_INLONG_STREAM_ID);
        this.dataKey = header.getOrDefault(PROXY_KEY_DATA, "");
        this.dateKey = header.getOrDefault(PROXY_KEY_DATE, "");
        this.taskID = header.get(PROXY_KEY_TASK_ID);
        // use the batch key of user and inlongStreamId to determine one batch
        if (StringUtils.isNotEmpty(taskID)) {
            this.batchKey = dataKey + taskID + inlongStreamId;
        } else {
            this.batchKey = dataKey + inlongStreamId;
        }
    }

    public ProxyMessage(Message message) {
        this(message.getBody(), message.getHeader());
        if (message instanceof DBSyncMessage) {
            this.logPosition = ((DBSyncMessage) message).getLogPosition();
            this.msgId = ((DBSyncMessage) message).getMsgId();
        }
    }

    public LogPosition getLogPosition() {
        return logPosition;
    }

    public long getMsgId() {
        return msgId;
    }

    public String getDataKey() {
        return dataKey;
    }

    /**
     * Get first line of body list
     *
     * @return first line of body list
     */
    @Override
    public byte[] getBody() {
        return body;
    }

    /**
     * Get header of message
     *
     * @return header
     */
    @Override
    public Map<String, String> getHeader() {
        return header;
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public String getInlongStreamId() {
        return inlongStreamId;
    }

    public String getBatchKey() {
        return batchKey;
    }

    public String getDateKey() {
        return dateKey;
    }

    public String getTaskID() {
        return taskID;
    }
}
