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

package org.apache.inlong.agent.mysql.connector.driver.packets.client;

import org.apache.inlong.agent.mysql.connector.driver.packets.CommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * COM_BINLOG_DUMP
 */
public class BinlogDumpCommandPacket extends CommandPacket {

    public long binlogPosition;
    public long slaveServerId;
    public String binlogFileName;

    public BinlogDumpCommandPacket() {
        setCommand((byte) 0x12);
    }

    public void fromBytes(byte[] data) {
        // bypass
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            command
     *  n                            arg
     *  --------------------------------------------------------
     *  Bytes                        Name
     *  -----                        ----
     *  4                            binlog position to start at (little endian)
     *  2                            binlog flags (currently not used; always 0)
     *  4                            server_id of the slave (little endian)
     *  n                            binlog file name (optional)
     *
     * </pre>
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 0. write command number
        out.write(getCommand());
        // 1. write 4 bytes bin-log position to start at
        ByteHelper.writeUnsignedIntLittleEndian(binlogPosition, out);
        // 2. write 2 bytes bin-log flags
        out.write(0x00);
        out.write(0x00);
        // 3. write 4 bytes server id of the slave
        ByteHelper.writeUnsignedIntLittleEndian(this.slaveServerId, out);
        // 4. write bin-log file name if necessary
        if (StringUtils.isNotEmpty(this.binlogFileName)) {
            out.write(this.binlogFileName.getBytes());
        }
        return out.toByteArray();
    }

}
