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

/**
 * <pre>
 * Type Of Result Packet       Hexadecimal Value Of First Byte (field_count)
 * ---------------------------------------------------------------------------
 * Result Set Packet           1-250 (first byte of Length-Coded Binary)
 * </pre>
 *
 * The sequence of result set packet:
 *
 * <pre>
 *   (Result Set Header Packet)  the number of columns
 *   (Field Packets)             column descriptors
 *   (EOF Packet)                marker: end of Field Packets
 *   (Row Data Packets)          row contents
 * (EOF Packet)                marker: end of Data Packets
 *
 * </pre>
 */
public class ResultSetHeaderPacket extends PacketWithHeaderPacket {

    private long columnCount;
    private long extra;

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        byte[] colCountBytes = ByteHelper.readBinaryCodedLengthBytes(data, index);
        columnCount = ByteHelper.readLengthCodedBinary(colCountBytes, index);
        index += colCountBytes.length;
        if (index < data.length - 1) {
            extra = ByteHelper.readLengthCodedBinary(data, index);
        }
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public long getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(long columnCount) {
        this.columnCount = columnCount;
    }

    public long getExtra() {
        return extra;
    }

    public void setExtra(long extra) {
        this.extra = extra;
    }

    public String toString() {
        return "ResultSetHeaderPacket [columnCount=" + columnCount + ", extra=" + extra + "]";
    }

}
