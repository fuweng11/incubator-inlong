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

import java.nio.ByteBuffer;
import java.util.UUID;

public class GtidLogEvent extends LogEvent {

    /// Length of the commit_flag in event encoding
    public static final int ENCODED_FLAG_LENGTH = 1;
    /// Length of SID in event encoding
    public static final int ENCODED_SID_LENGTH = 16;
    public static final int LOGICAL_TIMESTAMP_TYPECODE_LENGTH = 1;
    public static final int LOGICAL_TIMESTAMP_LENGTH = 16;
    public static final int LOGICAL_TIMESTAMP_TYPECODE = 2;

    public static final int[] BYTES_PER_SECTION = {4, 2, 2, 2, 6};

    private boolean commitFlag;

    private UUID sid;
    private long gno;
    private long lastCommitted = 0;
    private long sequenceNumber = 0;

    public GtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;

        buffer.position(commonHeaderLen);
        commitFlag = (buffer.getUint8() != 0); // ENCODED_FLAG_LENGTH

        byte[] bytes = buffer.getData(ENCODED_SID_LENGTH);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        sid = new UUID(high, low);

        gno = buffer.getLong64();

        if (buffer.hasRemaining() && buffer.remaining() > LOGICAL_TIMESTAMP_LENGTH
                && buffer.getUint8() == LOGICAL_TIMESTAMP_TYPECODE) {
            lastCommitted = buffer.getLong64();
            sequenceNumber = buffer.getLong64();
        }
    }

    public boolean isCommitFlag() {
        return commitFlag;
    }

    public long getGno() {
        return gno;
    }

    public UUID getSid() {
        return sid;
    }

    public Long getLastCommitted() {
        return lastCommitted;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getGtidStr() {
        return sid.toString() + ":" + gno;
    }

    @Override
    public String toString() {
        return "GtidLogEvent{ commitFlag=" + commitFlag + ", sid=" + sid + ", gno=" + gno + ", lastCommitted="
                + lastCommitted + ", sequenceNumber=" + sequenceNumber + '}';
    }
}
