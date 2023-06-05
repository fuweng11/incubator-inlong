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

package org.apache.inlong.agent.utils;

import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.common.protocol.CanalEntry.RowChange;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.Column;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.RowData;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.common.pojo.agent.dbsync.DBServerInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.CompressionType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.NULL_STRING;

public class DBSyncUtils {

    public static final int[] BYTES_PER_SECTION = {4, 2, 2, 2, 6};
    public static final String HEX_STRING_LOW = "0123456789abcdef";
    protected static final Logger LOGGER = LogManager.getLogger(DBSyncUtils.class);
    private static final AtomicLong RANDOM_INDEX = new AtomicLong(0);

    public static long generateSlaveId(String jobName, String serverId) {
        long result = 0;
        String ip = AgentUtils.getLocalIp();
        try {
            if (Objects.equals(ip, "127.0.0.1") || Objects.equals(ip, "0.0.0.0")) {
                throw new Exception("conflict address");
            } else {
                result = DBSyncUtils.ipStr2Int(ip);
            }
        } catch (Exception e) {
            LOGGER.warn("invalid local ip: {}, slave_id will use a random number.", ip, e);
            Random r = new Random();
            result = 1000L + r.nextInt(1024);
        }
        if (StringUtils.isNotEmpty(serverId)) {
            try {
                result += DBSyncUtils.serverId2Int(serverId);
                result += RANDOM_INDEX.addAndGet(1);
            } catch (Exception e) {
                LOGGER.warn("invalid server Id: {}, slave_id will use a random number.", serverId, e);
                Random r = new Random();
                result += r.nextInt(1024);
            }
        } else {
            Random r = new Random();
            result += r.nextInt(1024);
        }
        LOGGER.info("{} create mysql slave_id is {}", jobName, result);
        return result;
    }

    public static String getExceptionStack(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String exceptStr = null;
        try {
            e.printStackTrace(pw);
            exceptStr = sw.toString();
        } catch (Exception ex) {
            LOGGER.error("printStackTrace error: ", ex);
        } finally {
            try {
                pw.close();
                sw.close();
            } catch (Exception ex) {
                LOGGER.error("close writer error: ", ex);
            }
        }
        return exceptStr;
    }

    public static boolean compareEntry(Entry entry1, Entry entry2) {

        if (!entry1.getHeader().getSchemaName().equals(entry2.getHeader().getSchemaName())) {
            return false;
        }

        if (!entry1.getHeader().getTableName().equals(entry2.getHeader().getTableName())) {
            return false;
        }

        try {
            RowChange rowChage1 = RowChange.parseFrom(entry1.getStoreValue());
            RowChange rowChage2 = RowChange.parseFrom(entry2.getStoreValue());
            if (rowChage1.getEventType() != rowChage2.getEventType()
                    || rowChage1.getRowDatasList().size() != rowChage2.getRowDatasList().size()) {
                return false;
            }

            // rowChage1.get
            for (int i = 0; i < rowChage1.getRowDatasList().size(); i++) {
                RowData row1 = rowChage1.getRowDatasList().get(i);
                RowData row2 = rowChage2.getRowDatasList().get(i);
                List<Column> beforCloum1 = row1.getBeforeColumnsList();
                List<Column> afterCloum1 = row1.getAfterColumnsList();

                List<Column> beforCloum2 = row2.getBeforeColumnsList();
                List<Column> afterCloum2 = row2.getAfterColumnsList();

                if (beforCloum1.size() != beforCloum2.size()) {
                    return false;
                }

                for (int j = 0; j < beforCloum1.size(); j++) {
                    if (!Objects.equals(beforCloum1.get(j), beforCloum2.get(j))) {
                        return false;
                    }
                }

                if (afterCloum1.size() != afterCloum2.size()) {
                    return false;
                }

                for (int k = 0; k < afterCloum1.size(); k++) {
                    if (!Objects
                            .equals(afterCloum1.get(k).getValue(), afterCloum2.get(k).getValue())) {
                        return false;
                    }
                }
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("parse rowdata exception: ", e);
        }
        return true;
    }

    public static boolean checkValidateDbInfo(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        boolean result = true;
        if (taskConf != null) {
            DBServerInfo dbConf = taskConf.getDbServerInfo();
            if (dbConf == null) {
                future.completeExceptionally(new DataSourceConfigException("db config is null!"));
                result = false;
            } else if (!checkDbUrl(dbConf.getUrl())
                    || StringUtils.isEmpty(taskConf.getDbName())
                    || StringUtils.isEmpty(dbConf.getUsername()) || StringUtils.isEmpty(dbConf.getPassword())) {
                future.completeExceptionally(new DataSourceConfigException("db config has error! "
                        + "dbName=" + taskConf.getDbName() + ", dbUrl=" + dbConf.getUrl()
                        + ", userName=" + dbConf.getUsername() + ", password = " + dbConf.getPassword()));
                result = false;
            }
        } else {
            future.completeExceptionally(new DataSourceConfigException("taskConf is null"));
            result = false;
        }
        // check data report info
        if (!taskConf.isValid()) {
            future.completeExceptionally(new DataSourceConfigException("taskConf dataReport info has error"));
            result = false;
        }
        return result;
    }

    public static char getSeperator(String sepStr) {
        char defaultValue = (char) 0x01;
        if (sepStr == null) {
            return defaultValue;
        }

        sepStr = sepStr.trim().toLowerCase();
        if (sepStr.startsWith("0x")) {
            sepStr = sepStr.replaceFirst("0x", "").trim();
            try {
                int intSplitter = Integer.parseInt(sepStr, 16);
                if (intSplitter > 0 && intSplitter < 255) {
                    defaultValue = (char) intSplitter;
                }
            } catch (NumberFormatException ne) {
                LOGGER.error(DBSyncUtils.getExceptionStack(ne));
            }
        }
        return defaultValue;
    }

    public static byte[] intToBigEndian(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[0] = (byte) (n >> 24 & 0xff);
        return b;
    }

    public static int bigEndianToInt(byte[] res) {

        return (res[3] & 0xff) | ((res[2] << 8) & 0xff00) | ((res[1] << 24) >>> 8) | (res[0] << 24);
    }

    public static void sleep(long millseconds) {
        try {
            Thread.sleep(millseconds);
        } catch (InterruptedException ie) {
            LOGGER.error("sleep error", ie);
        }
    }

    public static String byte2String(byte bb) {
        String hexString = "0123456789ABCDEF";
        return "0x" + hexString.charAt((bb & 0xf0) >> 4) + hexString.charAt((bb & 0x0f));
    }

    public static String byteArrayToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte bb : bytes) {
            sb.append(byte2String(bb)).append(" ");
        }
        return sb.toString();
    }

    public static String bytes2string(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (; index < bytes.length - 1; index += 2) {
            sb.append(String.format("%02x%02x ", bytes[index], bytes[index + 1]));
        }
        if (bytes.length % 2 == 1) {
            sb.append(String.format("%02x", bytes[bytes.length - 1]));
        }
        return sb.toString();
    }

    public static String genUuid(byte[] bytes) {

        StringBuilder sb = new StringBuilder();

        int index = 0;
        for (int i = 0; i < BYTES_PER_SECTION.length; i++) {
            if (i > 0) {
                sb.append("-");
            }
            for (int j = 0; j < BYTES_PER_SECTION[i]; j++) {
                int b = bytes[index++] & 0xff;
                sb.append(HEX_STRING_LOW.charAt(b >>> 4));
                sb.append(HEX_STRING_LOW.charAt(b & 0xf));
            }

        }
        return sb.toString();
    }

    public static long ipStr2Int(String ip) throws UnknownHostException {
        long result = 0;
        InetAddress ipv = InetAddress.getByName(ip);
        for (byte b : ipv.getAddress()) {
            result = result << 8 | (b & 0xFF);
        }
        return result;
    }

    public static long serverId2Int(String serverId) {
        long result = 0;
        for (byte b : serverId.getBytes(StandardCharsets.UTF_8)) {
            result = result << 8 | (b & 0xFF);
        }
        result %= SnowFlake.MAX_MACHINE_NUM;
        return result;
    }

    public static String ipStr2HexStr(String ip) {
        StringBuffer sb = new StringBuffer();
        if (ip == null) {
            return "";
        }

        String[] ipSetStr = ip.split("\\.");
        if (ipSetStr.length != 4) {
            return "";
        }

        String byteStr = null;
        for (int i = 0; i < 4; i++) {
            byteStr = String.valueOf((byte) (Integer.parseInt(ipSetStr[i]) & 0xFF));
            if (byteStr.length() == 1) {
                sb.append("0").append(byteStr);
            } else {
                sb.append(byteStr);
            }
        }
        return sb.toString();
    }

    public static String getNullFieldReplaceStr(String character) {
        if (character == null) {
            return null;
        }

        String splitterStr = null;
        StringBuilder sb = new StringBuilder();
        if (character.startsWith("0x")) {
            character = character.replaceFirst("0x", "").trim();
            try {
                int intSplitter = Integer.parseInt(character, 16);
                if (intSplitter > 0 && intSplitter < 255) {
                    splitterStr = sb.append((char) intSplitter).toString();
                }
            } catch (NumberFormatException ne) {
                LOGGER.error(getExceptionStack(ne));
            }
        } else {
            splitterStr = character;
        }

        return splitterStr;
    }

    public static String getNextBinlogFileName(String binlogFileName) {
        String binlogFileNamePrefix = binlogFileName.substring(0, binlogFileName.lastIndexOf(".") + 1);
        int binlogSeqNum = Integer.parseInt(binlogFileName.substring(binlogFileName.lastIndexOf(".") + 1));
        int nexBinlogSeqIndex = binlogSeqNum + 1;

        return binlogFileNamePrefix + String.format("%06d", nexBinlogSeqIndex);
    }

    public static LocalDateTime convertFrom(long millisec) {
        return Instant.ofEpochMilli(millisec).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    public static boolean isCollectionsEmpty(Collection coll) {
        return (coll == null || coll.isEmpty());
    }

    // TODO:add send big field metrics
    // public static void sendBigFieldMetrics(long dataTime, MysqlTableConf mysqlTableConf,
    // FieldMeta fieldMeta, long filedLength) {
    // if (fieldMeta == null) {
    // return;
    // }
    //
    // DBSyncConf instance = DBSyncConf.getInstance(null);
    // String metricsBid = instance.getStringVar(METRICS_BID);
    // String metricsTid = instance.getStringVar(METRICS_BIG_FIELD_TID);
    // String localIp = instance.getLocalIp();
    // String businessId = mysqlTableConf.getGroupId();
    // String iname = mysqlTableConf.getStreamId();
    //
    // StringJoiner joiner = new StringJoiner(BUS_DELIMITER);
    // String message = joiner.add(localIp).add(businessId).add(iname)
    // .add(fieldMeta.getColumnName()).add(String.valueOf(filedLength)).add(String.valueOf(dataTime))
    // .toString();
    // logger.error("send big field metrics {}", message);
    //
    // sendMetricsToBus(metricsBid, metricsTid, message);
    //
    // }

    // TODO:add send metrics to bus
    // public static void sendMetricsToBus(String metricsBid, String metricsTid, String message) {
    // ProxyEvent event = new ProxyEvent(message, metricsBid, metricsTid, System.currentTimeMillis(), 20 * 1000);
    // BusMessageQueueCallback callback = new BusMessageQueueCallback(event);
    // HttpProxySender busSender = JobMonitor.getProxySender();
    // busSender.asyncSendMessage(message, metricsBid, metricsTid, System.currentTimeMillis(),
    // 20 * 1000, TimeUnit.MILLISECONDS, callback);
    // }

    /**
     * determine whether tdm pass NULL string
     */
    public static boolean isStrNull(String str) {
        if (str == null) {
            return false;
        }
        return str.equalsIgnoreCase(NULL_STRING);
    }

    public static boolean checkDbUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return false;
        }
        String[] ipPort = url.split(":");
        if (ipPort.length < 2) {
            return false;
        }
        if (!StringUtils.isNumeric(ipPort[1])) {
            return false;
        }
        int port = Integer.parseInt(ipPort[1]);
        if (port < 0) {
            return false;
        }
        return true;
    }

    public static String getHost(String url) {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        String[] ipPort = url.split(":");
        return ipPort[0];
    }

    public static int getPort(String url) {
        if (StringUtils.isBlank(url)) {
            return -1;
        }
        String[] ipPort = url.split(":");
        if (ipPort.length < 2) {
            LOGGER.error("invalid port, url {}", url);
            return -1;
        }
        return Integer.parseInt(ipPort[1]);
    }

    /**
     * convert string compress type to enum compress type
     * @param type
     * @return
     */
    public static CompressionType convertType(String type) {
        switch (type) {
            case "lz4":
                return CompressionType.LZ4;
            case "zlib":
                return CompressionType.ZLIB;
            case "zstd":
                return CompressionType.ZSTD;
            case "snappy":
                return CompressionType.SNAPPY;
            case "none":
            default:
                return CompressionType.NONE;
        }
    }
}
