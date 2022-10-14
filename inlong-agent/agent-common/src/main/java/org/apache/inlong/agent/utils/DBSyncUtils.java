package org.apache.inlong.agent.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.constant.DBSyncConfConstants;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.common.pojo.agent.dbsync.DBServerInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.CompressionType;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBSyncUtils {

    public static final int[] BYTES_PER_SECTION = {4, 2, 2, 2, 6};
    public static final String hexStringlow = "0123456789abcdef";
    protected static final Logger logger = LogManager.getLogger(DBSyncUtils.class);
    private static String hexString = "0123456789ABCDEF";

    public static String getExceptionStack(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String exceptStr = null;
        try {
            e.printStackTrace(pw);
            exceptStr = sw.toString();
        } catch (Exception ex) {
//			ex.printStackTrace();
            logger.error("printStackTrace error: ", ex);
        } finally {
            try {
                pw.close();
                sw.close();
            } catch (Exception ex) {
//				ex.printStackTrace();
                logger.error("close writer error: ", ex);
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
                logger.error(DBSyncUtils.getExceptionStack(ne));
            }
        }
        return defaultValue;
    }

    /**
     * 十进制 ${1, 99} = 1, 2, 3, ... 98, 99 <br>
     * 带占位符的十进制 ${01, 99} = 01, 02, ... 98, 99 <br>
     *
     * 十六进制 ${0x0,0xff} = 1, 2, ... fe, ff <br>
     * 带占位符的十六进制 ${0x00,0xff} = 01, 02, ... fe, ff <br>
     *
     * 八进制 ${O1,O10} = 1, 2,... 7, 10<br>
     * 带占位符的八进制 ${O01,O10} = 01, 02,... 07, 10<br>
     *
     * 带步进的例子：<br>
     * test_${0x00,0x12,5} = test_00, test_05, test_0a, test_0f<br>
     *
     * @param str
     * @return
     */
    public static String[] replaceDynamicSequence(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        Pattern pattern = Pattern.
                compile("\\$\\{(((0x)|(0X)|o|O)??[0-9a-fA-F]+?) *, *(((0x)|(0X)|o|O)??[0-9a-fA-F]+?) *(, *[0-9]*?)??\\}");
        Pattern octPattern = Pattern.compile("^o[0-7]+?$");
        Pattern decPattern = Pattern.compile("^[0-9]+?$");
        StringBuffer sb = new StringBuffer();
        int index = 0;
        /* find need replace number string */
        Matcher matcher = pattern.matcher(str);
        ArrayList<String> startNum = new ArrayList<String>();
        ArrayList<String> endNum = new ArrayList<String>();
        ArrayList<Integer> modes = new ArrayList<Integer>();
        ArrayList<Integer> steps = new ArrayList<Integer>();
        while (matcher.find()) {
            String matchStr = matcher.group(0);
            matchStr = StringUtils.strip(matchStr, "${");
            matchStr = StringUtils.strip(matchStr, "}");
            String[] patterns = matchStr.split(",");
            String startStr = patterns[0].trim().toLowerCase();
            String endStr = patterns[1].trim().toLowerCase();
            int step = 1;
            if (patterns.length >= 3) {
                String stepStr = patterns[2].trim();
                if (stepStr.length() > 0) {
                    step = Integer.parseInt(stepStr);
                }
            }

            boolean bFound = false;
            int mode = -1;

            /* matche hex string */
            if (startStr.startsWith("0x") && endStr.startsWith("0x")) {
                bFound = true;
                mode = 16;
            }
            /* matche oct string */
            else if (startStr.startsWith("o") && endStr.startsWith("o")) {
                Matcher startMatch = octPattern.matcher(startStr);
                Matcher endMatch = octPattern.matcher(endStr);
                if (startMatch.find() && endMatch.find()) {
                    bFound = true;
                    mode = 8;
                }
            }
            /* matche dec string */
            else {
                Matcher startMatch = decPattern.matcher(startStr);
                Matcher endMatch = decPattern.matcher(endStr);
                if (startMatch.find() && endMatch.find()) {
                    bFound = true;
                    mode = 10;
                }
            }

            /* if not matche oct, dec, hex; not do anything */
            /* if matched, bFound = true */
            if (bFound) {
                startNum.add(startStr);
                endNum.add(endStr);
                modes.add(mode);
                steps.add(step);
                matcher.appendReplacement(sb, "\\${" + (index++) + "}");
            }
        }
        matcher.appendTail(sb);

        @SuppressWarnings("unchecked")
        ArrayList<String>[] tempArray = new ArrayList[startNum.size() + 1];
        tempArray[0] = new ArrayList<String>();
        tempArray[0].add(sb.toString());
        for (index = 0; index < startNum.size(); index++) {
            String start = startNum.get(index);
            String end = endNum.get(index);
            int mode = modes.get(index);
            int step = steps.get(index);
            tempArray[index + 1] = new ArrayList<String>();
            boolean lengthEquals = start.length() == end.length();
            for (String currentPath : tempArray[index]) {
                for (int i = parseInt(start); i <= parseInt(end); i = i + step) {
                    tempArray[index + 1].add(currentPath.replaceAll("\\$\\{" + index + "\\}",
                            formart(i, lengthEquals, end.length(), mode)));
                }
            }
        }

        return tempArray[startNum.size()].toArray(new String[0]);
    }

    /**
     * @param dirName
     * @return
     */
    public static boolean delDirs(String dirName) {
//        // only delete regular files
//        try {
//            Files.list(Paths.get(dirName))
////                    .peek(System.out::println)
//                    .filter(Files::isRegularFile)
//                    .map(Path::toFile)
//                    .forEach(File::delete);
////            Files.delete(Paths.get(dirName));
//            new File(dirName).delete();
//        } catch (IOException e) {
//            logger.error("delete dir {} error: ", dirName, e);
//            return false;
//        }
//        return true;

//////////////////////////////////////////////////////////////////////
// delete all file (include directory)
        try {
            Files.walk(Paths.get(dirName))
                    .sorted(Comparator.reverseOrder())
//                    .peek(System.out::println)
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            logger.error("delete dir {} error: ", dirName, e);
            return false;
        }
        return true;

///////////////////////////////////////////////////////////////////////
//        File dir = new File(dirName);
//        if (!dir.exists()) {
//            return true;
//        }
//        try {
//            for (File eachFile : Objects.requireNonNull(dir.listFiles())) {
//                if (eachFile.isDirectory()) {
//                    delDirs(eachFile.getAbsolutePath());
//                } else {
//                    eachFile.delete();
//                }
//            }
//            dir.delete();
//        } catch (Exception e) {
//            logger.error("delete directory {} error ", dirName, e);
//            return false;
//        }
//        return true;
    }

    /**
     * @param parseStr string
     * @return number value
     */
    private static int parseInt(String parseStr) {

        int parseValue = -1;

        if (parseStr.startsWith("0x")) {
            parseStr = parseStr.substring(2).trim();
            parseValue = Integer.parseInt(parseStr, 16);
        } else if (parseStr.startsWith("o")) {
            parseStr = parseStr.substring(1).trim();
            parseValue = Integer.parseInt(parseStr, 8);
        } else {
            parseValue = Integer.parseInt(parseStr);
        }

        return parseValue;
    }

    /**
     * @param num value
     * @param lengthEquals
     * @param length
     * @param mode =8, dec=10, hex=16
     * @return
     */
    private static String formart(int num, boolean lengthEquals, int length, int mode) {
        String numStr = null;
        if (mode == 16) {
            numStr = Integer.toHexString(num);
            /* sub hex head '0x' */
            length = length - 2;
        } else if (mode == 8) {
            numStr = Integer.toOctalString(num);
            /* sub oct head 'o' */
            length = length - 1;
        } else {
            numStr = String.valueOf(num);
        }

        /* append string length for lengthEquals = true */
        if (lengthEquals && numStr != null) {
            if (numStr.length() < length) {
                StringBuffer numberFormatStr = new StringBuffer();
                for (int i = 0; i < length - numStr.length(); i++) {
                    numberFormatStr.append(0);
                }
                numberFormatStr.append(numStr);
                numStr = numberFormatStr.toString();
            }
        }
        return numStr;
    }

    public static List<String> queryBinaryLogs(MysqlConnection connection) {
        ArrayList<String> fileNameSet = new ArrayList<String>();
        try {
            ResultSetPacket packet = connection.query("show binary logs");
            List<String> returnValues = packet.getFieldValues();

            for (int i = 0; i < returnValues.size(); i = i + 2) {
                fileNameSet.add(returnValues.get(i));
            }

        } catch (IOException e) {
            logger.error("Query Binary logs Error : {}", DBSyncUtils.getExceptionStack(e));
        }

        return fileNameSet;
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
                sb.append(hexStringlow.charAt(b >>> 4));
                sb.append(hexStringlow.charAt(b & 0xf));
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
                logger.error(getExceptionStack(ne));
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

    /**
     * convert string compress type to enum compress type
     *
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

    /**
     * get real topic from pulsar topic
     */
    public static String getPulsarTopic(String topic) {
        if (StringUtils.isEmpty(topic)) {
            return null;
        }
        String[] split = topic.split("/");
        return split[2];
    }


    /**
     * send big filed metrics
     *
     * @return
     */
    //TODO:add
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

    /**
     * send metrics to bus common
     *
     * @param metricsBid
     * @param metricsTid
     * @param message
     */
    //TODO:add
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
        return str.equalsIgnoreCase(DBSyncConfConstants.NULL_STRING);
    }

    //TODO
    public static String getHost(String url){
        String[] ipPort = url.split(":");
        return ipPort[0];
    }

    //TODO
    public static int getPort(String url){
        String[] ipPort = url.split(":");
        return Integer.parseInt(ipPort[1]);
    }

}
