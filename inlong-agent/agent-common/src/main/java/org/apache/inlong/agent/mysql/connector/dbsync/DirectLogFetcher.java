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

package org.apache.inlong.agent.mysql.connector.dbsync;

import org.apache.inlong.agent.mysql.connector.binlog.LogFetcher;
import org.apache.inlong.agent.mysql.connector.exception.BinlogMissException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

public class DirectLogFetcher extends LogFetcher {

    // Master heartbeat interval
    public static final int MASTER_HEARTBEAT_PERIOD_SECONDS = 15;
    /**
     * Command to dump binlog
     */
    public static final byte COM_BINLOG_DUMP = 18;
    /**
     * Packet header sizes
     */
    public static final int NET_HEADER_SIZE = 4;
    public static final int SQLSTATE_LENGTH = 5;
    /**
     * Packet offsets
     */
    public static final int PACKET_LEN_OFFSET = 0;
    public static final int PACKET_SEQ_OFFSET = 3;
    /**
     * Maximum packet length
     */
    public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);
    protected static final Logger logger = LogManager.getLogger(DirectLogFetcher.class);
    static final int SO_TIMEOUT = 1000;
    // +10s to ensure timeout > heartbeat interval
    private static final int READ_TIMEOUT_MILLISECONDS = (MASTER_HEARTBEAT_PERIOD_SECONDS + 10) * 1000;
    private SocketChannel channel;
    private InputStream input;
    private OutputStream output;

    public DirectLogFetcher() {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity) {
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor) {
        super(initialCapacity, growthFactor);
    }

    public void start(SocketChannel channel) throws IOException {
        this.channel = channel;
        this.input = new BufferedInputStream(channel.socket().getInputStream(), 16384);
        this.output = channel.socket().getOutputStream();
    }

    public boolean fetch() throws IOException {

        long fetchBytes = 0;

        try {
            // Fetching packet header from input.
            if (!fetch0(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }
            fetchBytes = fetchBytes + NET_HEADER_SIZE;

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET);
            int netnum = getUint8(PACKET_SEQ_OFFSET);
            if (!fetch0(NET_HEADER_SIZE, netlen)) {
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }
            fetchBytes = fetchBytes + netlen;

            // Detecting error code.
            final int mark = getUint8(NET_HEADER_SIZE);
            if (mark != 0) {
                // error from master
                if (mark == 255) {
                    // Indicates an error, for example trying to fetch from wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    if (errno == 1236 || 29 == errno || 1373 == errno) {
                        throw new BinlogMissException(
                                "Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                                        + " errmsg = " + errmsg);
                    } else {
                        throw new IOException(
                                "Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                                        + " errmsg = " + errmsg);
                    }
                } else if (mark == 254) {
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent" + " master disconnected.");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                            + ", len = " + netlen);
                }
            }

            // The first packet is a multi-packet, concatenate the packets.
            while (netlen == MAX_PACKET_LENGTH) {
                if (!fetch0(0, NET_HEADER_SIZE)) {
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }
                fetchBytes = fetchBytes + NET_HEADER_SIZE;

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
                fetchBytes = fetchBytes + netlen;
            }

            // Preparing buffer variables to decoding.
            origin = NET_HEADER_SIZE + 1;
            position = origin;
            limit -= origin;

            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (ClosedByInterruptException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);
        read(buffer, off, len, READ_TIMEOUT_MILLISECONDS);

        if (limit < off + len) {
            limit = off + len;
        }
        return true;
    }

    private void read(byte[] data, int off, int len, int timeout) throws IOException {
        InputStream input = this.input;
        int accTimeout = 0;
        if (input == null) {
            throw new SocketException("Socket already closed.");
        }

        int n = 0;
        while (n < len && accTimeout < timeout) {
            try {
                int read = input.read(data, off + n, len - n);
                if (read > -1) {
                    n += read;
                } else {
                    throw new IOException("EOF encountered.");
                }
            } catch (SocketTimeoutException te) {
                if (Thread.interrupted()) {
                    throw new ClosedByInterruptException();
                }
                accTimeout += SO_TIMEOUT;
            }
        }

        if (n < len && accTimeout >= timeout) {
            throw new SocketTimeoutException("Timeout occurred, failed to read " + len + " bytes in " + timeout
                    + " milliseconds.");
        }
    }

    public void close() throws IOException {
        // do nothing
        this.input = null;
        this.output = null;
    }

}