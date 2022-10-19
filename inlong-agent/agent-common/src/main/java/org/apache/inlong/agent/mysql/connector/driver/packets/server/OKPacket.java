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

package org.apache.inlong.agent.mysql.connector.driver.packets.server;

import org.apache.inlong.agent.mysql.connector.driver.packets.PacketWithHeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Aka. OK packet
 */
public class OKPacket extends PacketWithHeaderPacket {

    public byte fieldCount;
    public byte[] affectedRows;
    public byte[] insertId;
    public int serverStatus;
    public int warningCount;
    public String message;

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1   (Length Coded Binary)   field_count, always = 0
     *  1-9 (Length Coded Binary)   affected_rows
     *  1-9 (Length Coded Binary)   insert_id
     *  2                           server_status
     *  2                           warning_count
     *  n   (until end of packet)   message
     * </pre>
     *
     * @throws IOException
     */
    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        // 1. read field count
        this.fieldCount = data[0];
        index++;
        // 2. read affected rows
        this.affectedRows = ByteHelper.readBinaryCodedLengthBytes(data, index);
        index += this.affectedRows.length;
        // 3. read insert id
        this.insertId = ByteHelper.readBinaryCodedLengthBytes(data, index);
        index += this.insertId.length;
        // 4. read server status
        this.serverStatus = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 5. read warning count
        this.warningCount = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 6. read message.
        this.message = new String(ByteHelper.readFixedLengthBytes(data, index, data.length - index));
        // end read
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public byte getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(byte fieldCount) {
        this.fieldCount = fieldCount;
    }

    public byte[] getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(byte[] affectedRows) {
        this.affectedRows = affectedRows;
    }

    public byte[] getInsertId() {
        return insertId;
    }

    public void setInsertId(byte[] insertId) {
        this.insertId = insertId;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public int getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String toString() {
        return "OKPacket [affectedRows=" + Arrays.toString(affectedRows) + ", fieldCount=" + fieldCount + ", insertId="
                + Arrays.toString(insertId) + ", message=" + message + ", serverStatus=" + serverStatus
                + ", warningCount=" + warningCount + "]";
    }

}
