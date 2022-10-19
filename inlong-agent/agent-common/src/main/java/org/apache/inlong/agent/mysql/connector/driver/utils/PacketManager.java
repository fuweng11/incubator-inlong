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

package org.apache.inlong.agent.mysql.connector.driver.utils;

import org.apache.inlong.agent.mysql.connector.driver.packets.HeaderPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class PacketManager {

    public static HeaderPacket readHeader(SocketChannel ch, int len) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.fromBytes(readBytesAsBuffer(ch, len).array());
        return header;
    }

    public static ByteBuffer readBytesAsBuffer(SocketChannel ch, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(len);
        while (buffer.hasRemaining()) {
            int readNum = ch.read(buffer);
            if (readNum == -1) {
                throw new IOException("Unexpected End Stream");
            }
        }
        return buffer;
    }

    public static void writePkg(SocketChannel ch, byte[]... srcs) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] src : srcs) {
            out.write(src);
        }
        ch.write(ByteBuffer.wrap(out.toByteArray()));
    }

    public static byte[] readBytes(SocketChannel ch, int len) throws IOException {
        return readBytesAsBuffer(ch, len).array();
    }

    /**
     * Since We r using blocking IO, so we will just write once and assert the length to simplify the read
     * operation.<br>
     * If the block write doesn't work as we expected, we will change this implementation as per the result.
     */
    public static void write(SocketChannel ch, ByteBuffer[] srcs) throws IOException {
        @SuppressWarnings("unused")
        long total = 0;
        for (ByteBuffer buffer : srcs) {
            total += buffer.remaining();
        }

        ch.write(srcs);
    }

    public static void write(SocketChannel ch, byte[] body) throws IOException {
        write(ch, body, (byte) 0);
    }

    public static void write(SocketChannel ch, byte[] body, byte packetSeqNumber) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(body.length);
        header.setPacketSequenceNumber(packetSeqNumber);
        write(ch, new ByteBuffer[]{ByteBuffer.wrap(header.toBytes()), ByteBuffer.wrap(body)});
    }
}
