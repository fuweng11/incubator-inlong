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

package org.apache.inlong.agent.mysql.connector.binlog.event;

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

/**
 * Execute_load_log_event.
 *
 * @version 1.0
 */
public final class ExecuteLoadLogEvent extends LogEvent {

    /* EL = "Execute Load" */
    public static final int EL_FILE_ID_OFFSET = 0;
    private final long fileId;

    public ExecuteLoadLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        buffer.position(commonHeaderLen + EL_FILE_ID_OFFSET);
        fileId = buffer.getUint32(); //  EL_FILE_ID_OFFSET
    }

    public final long getFileId() {
        return fileId;
    }
}
