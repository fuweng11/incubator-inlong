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

import org.apache.inlong.agent.mysql.connector.driver.packets.HeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.PacketWithHeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;
import org.apache.inlong.agent.mysql.connector.driver.utils.MSC;

import java.io.IOException;

/**
 * MySQL Handshake Initialization Packet.<br>
 */
public class HandshakeInitializationPacket extends PacketWithHeaderPacket {

    public byte protocolVersion = MSC.DEFAULT_PROTOCOL_VERSION;
    public String serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetNumber;
    public int serverStatus;
    public byte[] restOfScrambleBuff;

    public HandshakeInitializationPacket() {
    }

    public HandshakeInitializationPacket(HeaderPacket header) {
        super(header);
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            protocol_version
     *  n (Null-Terminated String)   server_version
     *  4                            thread_id
     *  8                            scramble_buff
     *  1                            (filler) always 0x00
     *  2                            server_capabilities
     *  1                            server_language
     *  2                            server_status
     *  13                           (filler) always 0x00 ...
     *  13                           rest of scramble_buff (4.1)
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read protocol_version
        protocolVersion = data[index];
        index++;
        // 2. read server_version
        byte[] serverVersionBytes = ByteHelper.readNullTerminatedBytes(data, index);
        serverVersion = new String(serverVersionBytes);
        index += (serverVersionBytes.length + 1);
        // 3. read thread_id
        threadId = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index += 4;
        // 4. read scramble_buff
        seed = ByteHelper.readFixedLengthBytes(data, index, 8);
        index += 8;
        index += 1; // 1 byte (filler) always 0x00
        // 5. read server_capabilities
        this.serverCapabilities = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 6. read server_language
        this.serverCharsetNumber = data[index];
        index++;
        // 7. read server_status
        this.serverStatus = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 8. bypass filtered bytes
        index += 13;
        // 9. read rest of scramble_buff
        this.restOfScrambleBuff = ByteHelper.readFixedLengthBytes(data, index, 12);
        // end read
    }

    /**
     * Bypass implementing it, 'cause nowhere to use it.
     */
    public byte[] toBytes() throws IOException {
        return null;
    }

}
