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

public class ErrorPacket extends PacketWithHeaderPacket {

    public byte fieldCount;
    public int errorNumber;
    public byte sqlStateMarker;
    public byte[] sqlState;
    public String message;

    /**
     * <pre>
     * VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1                           field_count, always = 0xff
     *  2                           errno
     *  1                           (sqlstate marker), always '#'
     *  5                           sqlstate (5 characters)
     *  n                           message
     * 
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        this.fieldCount = data[0];
        index++;
        // 2. read error no.
        this.errorNumber = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 3. read marker
        this.sqlStateMarker = data[index];
        index++;
        // 4. read sqlState
        this.sqlState = ByteHelper.readFixedLengthBytes(data, index, 5);
        index += 5;
        // 5. read message
        this.message = new String(ByteHelper.readFixedLengthBytes(data, index, data.length - index));
        // end read
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    @Override
    public String toString() {
        if (sqlState == null || message == null) {
            return "ErrorPacket [errorNumber=" + errorNumber + ", fieldCount=" + fieldCount;
        }
        return "ErrorPacket [errorNumber=" + errorNumber + ", fieldCount=" + fieldCount + ", message=" + message
                + ", sqlState=" + sqlStateToString() + ", sqlStateMarker=" + (char) sqlStateMarker + "]";
    }

    private String sqlStateToString() {
        StringBuilder builder = new StringBuilder(5);
        for (byte b : this.sqlState) {
            builder.append((char) b);
        }
        return builder.toString();
    }

}
