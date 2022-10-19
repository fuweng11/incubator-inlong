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
import org.apache.inlong.agent.mysql.connector.driver.packets.GTIDSet;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
 */
public class BinlogDumpGTIDCommandPacket extends CommandPacket {

    public static final int BINLOG_DUMP_NON_BLOCK = 0x01;
    public static final int BINLOG_THROUGH_POSITION = 0x02;
    public static final int BINLOG_THROUGH_GTID = 0x04;

    public long slaveServerId;
    public GTIDSet gtidSet;

    public BinlogDumpGTIDCommandPacket() {
        setCommand((byte) 0x1e);
    }

    @Override
    public void fromBytes(byte[] data) throws IOException {
    }

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 0. [1] write command number
        out.write(getCommand());
        // 1. [2] flags
        ByteHelper.writeUnsignedShortLittleEndian(BINLOG_THROUGH_GTID, out);
        // 2. [4] server-id
        ByteHelper.writeUnsignedIntLittleEndian(slaveServerId, out);
        // 3. [4] binlog-filename-len
        ByteHelper.writeUnsignedIntLittleEndian(0, out);
        // 4. [] binlog-filename
        // skip
        // 5. [8] binlog-pos
        ByteHelper.writeUnsignedInt64LittleEndian(4, out);
        // if flags & BINLOG_THROUGH_GTID {
        byte[] bs = gtidSet.encode();
        // 6. [4] data-size
        ByteHelper.writeUnsignedIntLittleEndian(bs.length, out);
        // 7, [] data
        // [8] n_sids
        // for n_sids {
        // [16] SID
        // [8] n_intervals
        // for n_intervals {
        // [8] start (signed)
        // [8] end (signed)
        // }
        // }
        out.write(bs);
        // }

        return out.toByteArray();
    }
}
