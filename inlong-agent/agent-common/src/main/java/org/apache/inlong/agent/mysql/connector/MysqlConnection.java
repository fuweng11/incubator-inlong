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

package org.apache.inlong.agent.mysql.connector;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.inlong.agent.mysql.connector.binlog.LogContext;
import org.apache.inlong.agent.mysql.connector.binlog.LogDecoder;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.FormatDescriptionLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.DirectLogFetcher;
import org.apache.inlong.agent.mysql.connector.driver.MysqlConnector;
import org.apache.inlong.agent.mysql.connector.driver.MysqlQueryExecutor;
import org.apache.inlong.agent.mysql.connector.driver.MysqlUpdateExecutor;
import org.apache.inlong.agent.mysql.connector.driver.packets.GTIDSet;
import org.apache.inlong.agent.mysql.connector.driver.packets.HeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.MysqlGTIDSet;
import org.apache.inlong.agent.mysql.connector.driver.packets.client.BinlogDumpCommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.client.BinlogDumpGTIDCommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.PacketManager;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.protocol.position.EntryPosition;
import org.apache.inlong.agent.mysql.relaylog.RelayLog;
import org.apache.inlong.agent.mysql.relaylog.exception.RelayLogPosErrorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.mysql.connector.dbsync.DirectLogFetcher.MASTER_HEARTBEAT_PERIOD_SECONDS;

public class MysqlConnection implements ErosaConnection {

    private static final Logger logger = LogManager.getLogger(MysqlConnection.class);

    private MysqlConnector connector;
    private long slaveId;
    private Charset charset = StandardCharsets.UTF_8;
    private BinlogFormat binlogFormat = BinlogFormat.ROW;
    private int binlogChecksum;
    private BinlogImage binlogImage;

    public MysqlConnection() {
    }

    public MysqlConnection(InetSocketAddress address, String username, String password) {

        connector = new MysqlConnector(address, username, password);
    }

    public MysqlConnection(InetSocketAddress address, String username, String password, byte charsetNumber,
            String defaultSchema) {
        connector = new MysqlConnector(address, username, password, charsetNumber, defaultSchema);
    }

    public void connect() throws IOException {
        connector.connect();
    }

    public void reconnect() throws IOException {
        connector.reconnect();
    }

    public void disconnect() throws IOException {
        connector.disconnect();
    }

    public boolean isConnected() {
        return connector.isConnected();
    }

    public ResultSetPacket query(String cmd) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.query(cmd);
    }

    public void update(String cmd) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void seek(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void seekAndCopyData(String binlogfilename, Long binlogPosition,
            SinkFunction func, RelayLog relayLog)
            throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        LogContext context = new LogContext();
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

        // a symbol for dump begin
        relayLog.putLog(getDumpBeginMagic(binlogChecksum));

        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decodeAndCopy(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }
            byte[] eventBody = decoder.getEventBody();
            if (eventBody != null) {
                if (!relayLog.putLog(eventBody)) {
                    throw new RelayLogPosErrorException("Relay log pos error, need redump");
                }
            } else {
                throw new CanalParseException("parse event, but can't get event data body");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    @SuppressWarnings({"rawtypes"})
    public void seekAndCopyData(EntryPosition dumpPosition, MysqlGTIDSet mysqlGTIDSet,
            SinkFunction func, RelayLog relayLog)
            throws IOException {
        seekAndCopyData(dumpPosition, mysqlGTIDSet, func, relayLog, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void seekAndCopyData(EntryPosition dumpPosition, MysqlGTIDSet mysqlGTIDSet,
            SinkFunction func, RelayLog relayLog, boolean gtidDump)
            throws IOException {
        boolean useGtid = false;
        updateSettings();
        loadBinlogChecksum();

        if (mysqlGTIDSet == null || mysqlGTIDSet.isEmpty() || !gtidDump) {
            sendBinlogDump(dumpPosition.getJournalName(), dumpPosition.getPosition());
        } else {
            MysqlGTIDSet purgedGtids = getGtidPurged();
            if (purgedGtids != null) {
                mysqlGTIDSet.update(purgedGtids);
            }
            sendBinlogDumpGTID(mysqlGTIDSet);
            useGtid = true;
        }

        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.GTID_LOG_EVENT);
        decoder.handle(LogEvent.PREVIOUS_GTIDS_LOG_EVENT);
        LogContext context = new LogContext();

        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

        // a symbol for dump begin
        relayLog.putLog(getDumpBeginMagic(binlogChecksum));

        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decodeAndCopy(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }
            byte[] eventBody = decoder.getEventBody();
            if (eventBody != null) {
                if (!relayLog.putLog(eventBody)) {
                    throw new RelayLogPosErrorException("Relay log pos error, need redump");
                }
            } else {
                throw new CanalParseException("parse event, but can't get event data body");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public void dump(long timestamp, SinkFunction func) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    @SuppressWarnings("rawtypes")
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDumpGTID(gtidSet);

        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        try {
            fetcher.start(connector.getChannel());
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
            // fix bug: #890
            // context.setGtidSet(gtidSet);
            while (fetcher.fetch()) {
                LogEvent event = null;
                event = decoder.decode(fetcher, context);

                if (event == null) {
                    throw new CanalParseException("parse failed");
                }

                if (!func.sink(event)) {
                    break;
                }
            }
        } finally {
            fetcher.close();
        }
    }

    private byte[] getDumpBeginMagic(int checkSum) {
        byte[] magic = null;
        switch (checkSum) {
            case LogEvent.BINLOG_CHECKSUM_ALG_OFF:
                magic = new byte[]{0x00, 0x00, 0x00, 0x00};
                break;
            case LogEvent.BINLOG_CHECKSUM_ALG_CRC32:
                magic = new byte[]{0x00, 0x00, 0x00, 0x01};
                break;
            default:
                magic = null;
                break;
        }

        return magic;
    }

    private void sendBinlogDump(String binlogfilename, Long binlogPosition) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP to {} with position:{}",
                connector.getAddress(), binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[]{
                ByteBuffer.wrap(binlogDumpHeader.toBytes()), ByteBuffer.wrap(cmdBody)});
    }

    private void sendBinlogDumpGTID(GTIDSet gtidSet) throws IOException {
        BinlogDumpGTIDCommandPacket binlogDumpCmd = new BinlogDumpGTIDCommandPacket();
        binlogDumpCmd.slaveServerId = this.slaveId;
        binlogDumpCmd.gtidSet = gtidSet;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP_GTID to {} with gtidSet {}",
                connector.getAddress(), binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[]{
                ByteBuffer.wrap(binlogDumpHeader.toBytes()), ByteBuffer.wrap(cmdBody)});
    }

    public MysqlConnection fork() {
        MysqlConnection connection = new MysqlConnection();
        connection.setCharset(getCharset());
        connection.setSlaveId(getSlaveId());
        connection.setConnector(connector.fork());
        return connection;
    }

    /**
     * the settings that will need to be checked or set:<br>
     * <ol>
     * <li>wait_timeout</li>
     * <li>net_write_timeout</li>
     * <li>net_read_timeout</li>
     * </ol>
     *
     * @throws IOException
     */
    private void updateSettings() throws IOException {
        try {
            update("set wait_timeout=9999999");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }
        try {
            update("set net_write_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            update("set net_read_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            update("set names 'binary'");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            update("set @master_binlog_checksum= @@global.binlog_checksum");
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn(ExceptionUtils.getFullStackTrace(e));
            }
        }

        /*
         * MASTER_HEARTBEAT_PERIOD sets the interval in seconds between replication heartbeats. Whenever the master's
         * binary log is updated with an event, the waiting period for the next heartbeat is reset. interval is a
         * decimal value having the range 0 to 4294967 seconds and a resolution in milliseconds; the smallest nonzero
         * value is 0.001. Heartbeats are sent by the master only if there are no unsent events in the binary log file
         * for a period longer than interval.
         */
        try {
            long periodNano = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
            update("SET @master_heartbeat_period=" + periodNano);
        } catch (Exception e) {
            logger.warn("update master_heartbeat_period failed", e);
        }

    }

    private MysqlGTIDSet getGtidPurged() {
        try {
            MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
            ResultSetPacket result = exector.query("show global variables like 'gtid_purged%'");
            if (result.getFieldValues().size() >= 2
                    && StringUtils.isNotBlank(result.getFieldValues().get(1))) {
                String gtidStr = result.getFieldValues().get(1);
                return MysqlGTIDSet.parse(gtidStr);
            }
        } catch (Exception e) {
            logger.error("get GTID_PURGED failed", e);
            return null;
        }
        return null;
    }

    private void loadBinlogFormat() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_format'");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result,"
                    + " so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }

        binlogFormat = BinlogFormat.valuesOf(columnValues.get(1));
        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    private void loadBinlogImage() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_row_image'");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            binlogImage = BinlogImage.FULL;
        } else {
            binlogImage = BinlogImage.valuesOf(columnValues.get(1));
        }

        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog image query result:" + rs.getFieldValues());
        }
    }

    private void loadBinlogChecksum() {
        ResultSetPacket rs = null;
        try {
            rs = query("select @master_binlog_checksum");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues != null && columnValues.size() >= 1 && columnValues.get(0) != null
                && columnValues.get(0).toUpperCase().equals("CRC32")) {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_CRC32;
        } else {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_OFF;
        }
    }

    public BinlogImage getBinlogImage() {
        if (binlogImage == null) {
            synchronized (this) {
                loadBinlogImage();
            }
        }

        return binlogImage;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public MysqlConnector getConnector() {
        return connector;
    }

    public void setConnector(MysqlConnector connector) {
        this.connector = connector;
    }

    public BinlogFormat getBinlogFormat() {
        if (binlogFormat == null) {
            synchronized (this) {
                loadBinlogFormat();
            }
        }

        return binlogFormat;
    }

    public int getBinlogChecksum() {
        return binlogChecksum;
    }

    // for test lynd
    public int getLocalPort() {

        if (connector == null) {
            return -1;
        }
        return connector.getLocalPort();
    }

    public static enum BinlogFormat {

        STATEMENT("STATEMENT"), ROW("ROW"), MIXED("MIXED");

        private String value;

        private BinlogFormat(String value) {
            this.value = value;
        }

        public static BinlogFormat valuesOf(String value) {
            BinlogFormat[] formats = values();
            for (BinlogFormat format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }

        public boolean isStatement() {
            return this == STATEMENT;
        }

        public boolean isRow() {
            return this == ROW;
        }

        public boolean isMixed() {
            return this == MIXED;
        }
    }

    public static enum BinlogImage {

        FULL("FULL"), MINIMAL("MINIMAL"), NOBLOB("NOBLOB");

        private String value;

        private BinlogImage(String value) {
            this.value = value;
        }

        public static BinlogImage valuesOf(String value) {
            BinlogImage[] formats = values();
            for (BinlogImage format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }

        public boolean isFull() {
            return this == FULL;
        }

        public boolean isMinimal() {
            return this == MINIMAL;
        }

        public boolean isNoBlob() {
            return this == NOBLOB;
        }
    }

}
