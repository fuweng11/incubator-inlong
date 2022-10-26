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

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.common.pojo.agent.dbsync.DBServerInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.inlong.agent.constant.AgentConstants.NULL_STRING;

public class DBSyncUtils {

    public static final int[] BYTES_PER_SECTION = {4, 2, 2, 2, 6};
    public static final String HEX_STRING_LOW = "0123456789abcdef";
    protected static final Logger LOGGER = LogManager.getLogger(DBSyncUtils.class);
    private static String hexString = "0123456789ABCDEF";

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

    public static boolean checkValidateDbInfo(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        boolean result = true;
        if (taskConf != null) {
            DBServerInfo dbConf = taskConf.getDbServerInfo();
            if (dbConf == null) {
                future.completeExceptionally(new DataSourceConfigException("db config is null!"));
                result = false;
            } else if (StringUtils.isEmpty(dbConf.getUrl())
                    || StringUtils.isEmpty(taskConf.getDbName())
                    || StringUtils.isEmpty(dbConf.getUsername()) || StringUtils.isEmpty(dbConf.getPassword())) {
                future.completeExceptionally(new DataSourceConfigException("db config is has "
                        + "error! dbName = " + taskConf.getDbName() + ",dbUrl = " + dbConf.getUrl() + ",userName = "
                        + dbConf.getUsername()
                        + ", password = " + dbConf.getPassword()
                ));
                result = false;
            } else if ("PULSAR".equalsIgnoreCase(taskConf.getMqType())
                    && StringUtils.isEmpty(taskConf.getMqResource())) {
                future.completeExceptionally(new DataSourceConfigException("pulsar address is "
                        + "empty!"));
                result = false;
            }
        } else {
            future.completeExceptionally(new DataSourceConfigException("taskConf is null"));
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

        int s = (res[3] & 0xff) | ((res[2] << 8) & 0xff00) | ((res[1] << 24) >>> 8) | (res[0] << 24);
        return s;
    }

    public static void sleep(long millseconds) {
        try {
            Thread.sleep(millseconds);
        } catch (InterruptedException ie) {
            LOGGER.error("sleep error", ie);
        }
    }

    public static String byte2String(byte bb) {
        StringBuilder sb = new StringBuilder();
        sb.append("0x");
        sb.append(hexString.charAt((bb & 0xf0) >> 4));
        sb.append(hexString.charAt((bb & 0x0f) >> 0));

        return sb.toString();
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
                sb.append("0").append(String.valueOf(byteStr));
            } else {
                sb.append(String.valueOf(byteStr));
            }
        }
        return sb.toString();
    }

    public static String getNullFieldReplaceStr(String character) {
        if (character == null) {
            return null;
        }

        String splitterStr = null;
        StringBuffer sb = new StringBuffer();
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
        String nextBinlogFileName = binlogFileNamePrefix + String.format("%06d", nexBinlogSeqIndex);

        return nextBinlogFileName;
    }

    public static LocalDateTime convertFrom(long millisec) {
        return Instant.ofEpochMilli(millisec).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    public static boolean isCollectionsEmpty(Collection coll) {
        return (coll == null || coll.isEmpty());
    }

    //TODO:add send big field metrics
//    public static void sendBigFieldMetrics(long dataTime, MysqlTableConf mysqlTableConf,
//            FieldMeta fieldMeta, long filedLength) {
//        if (fieldMeta == null) {
//            return;
//        }
//
//        DBSyncConf instance = DBSyncConf.getInstance(null);
//        String metricsBid = instance.getStringVar(METRICS_BID);
//        String metricsTid = instance.getStringVar(METRICS_BIG_FIELD_TID);
//        String localIp = instance.getLocalIp();
//        String businessId = mysqlTableConf.getGroupId();
//        String iname = mysqlTableConf.getStreamId();
//
//        StringJoiner joiner = new StringJoiner(BUS_DELIMITER);
//        String message = joiner.add(localIp).add(businessId).add(iname)
//                .add(fieldMeta.getColumnName()).add(String.valueOf(filedLength)).add(String.valueOf(dataTime))
//                .toString();
//        logger.error("send big field metrics {}", message);
//
//        sendMetricsToBus(metricsBid, metricsTid, message);
//
//    }

    //TODO:add send metrics to bus
//    public static void sendMetricsToBus(String metricsBid, String metricsTid, String message) {
//        ProxyEvent event = new ProxyEvent(message, metricsBid, metricsTid, System.currentTimeMillis(), 20 * 1000);
//        BusMessageQueueCallback callback = new BusMessageQueueCallback(event);
//        HttpProxySender busSender = JobMonitor.getProxySender();
//        busSender.asyncSendMessage(message, metricsBid, metricsTid, System.currentTimeMillis(),
//                20 * 1000, TimeUnit.MILLISECONDS, callback);
//    }

    /**
     * determine whether tdm pass NULL string
     */
    public static boolean isStrNull(String str) {
        if (str == null) {
            return false;
        }
        return str.equalsIgnoreCase(NULL_STRING);
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
}
