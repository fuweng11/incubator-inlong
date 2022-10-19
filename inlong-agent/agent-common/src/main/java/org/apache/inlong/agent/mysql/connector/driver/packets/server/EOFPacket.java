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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EOFPacket extends PacketWithHeaderPacket {

    public byte fieldCount;
    public int  warningCount;
    public int  statusFlag;

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                 Name
     *  -----                 ----
     *  1                     field_count, always = 0xfe
     *  2                     warning_count
     *  2                     Status Flags
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        fieldCount = data[index];
        index++;
        // 2. read warning count
        this.warningCount = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 3. read status flag
        this.statusFlag = ByteHelper.readUnsignedShortLittleEndian(data, index);
        // end read
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(5);
        out.write(this.fieldCount);
        ByteHelper.writeUnsignedShortLittleEndian(this.warningCount, out);
        ByteHelper.writeUnsignedShortLittleEndian(this.statusFlag, out);
        return out.toByteArray();
    }

}
