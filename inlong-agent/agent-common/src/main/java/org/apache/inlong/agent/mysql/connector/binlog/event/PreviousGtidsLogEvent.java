package org.apache.inlong.agent.mysql.connector.binlog.event;

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.driver.packets.MysqlGTIDSet;

/**
 * 
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class PreviousGtidsLogEvent  extends LogEvent {

//    public static final int     UUID_BYTE_LENGTH        =       16;
//    public static final int     GTID_BYTE_LENGTH        =        8;
//    public static final int     INTERNAL_BYTE_LENGTH    =        8;

//    private long nsids  = 0;
//    private Map<String, ArrayList<GtidPair>> gtidSetMap = new HashMap<String, ArrayList<GtidPair>>();

    private MysqlGTIDSet mysqlGTIDSet;


    public PreviousGtidsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
        // do nothing , just for mysql gtid search function
        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];

//        logger.info("PreviousGtidsLogEvent LogBuffer hexDump: " + DBSyncUtils.bytes2string(buffer.getData()));

//        buffer.position(commonHeaderLen + postHeaderLen);
//
//        nsids = buffer.getLong64();
//        for (long i = 0; i < nsids; i++) {
//            byte[] bytes = buffer.getData(UUID_BYTE_LENGTH);
////            buffer.forward(UUID_BYTE_LENGTH);
//            String uuid = DBSyncUtils.genUuid(bytes);
//
//            long nInternals = buffer.getLong64();
//
//            ArrayList<GtidPair> gtidSet = gtidSetMap.computeIfAbsent(uuid, k -> new ArrayList<>());
//
//            for (long n = 0; n < nInternals; n++) {
//                GtidPair pair = new GtidPair();
//                pair.start = buffer.getLong64();
//                pair.end = buffer.getLong64();
//                gtidSet.add(pair);
//            }
//        }
//
//        buffer.rewind();
        buffer.position(commonHeaderLen + postHeaderLen);
        mysqlGTIDSet = MysqlGTIDSet.parse(buffer);
    }

//    private static class GtidPair {
//        long start;
//        long end;
//
//        @Override
//        public String toString() {
//            return "GtidPair{" +
//                    "start=" + start +
//                    ", end=" + end +
//                    '}';
//        }
//    }


    public MysqlGTIDSet getMysqlGTIDSet() {
        return mysqlGTIDSet;
    }

    @Override
    public String toString() {

//        return "PreviousGtidsLogEvent{nsids=" + nsids + ", gtidSetMap="  + Arrays.toString(gtidSetMap.entrySet().toArray()) + ", MysqlGTIDSet=" + mysqlGTIDSet.toString();

//        StringBuilder sb = new StringBuilder();
//        for (Map.Entry<String, ArrayList<GtidPair>> entry : gtidSetMap.entrySet()) {
//            sb.append(entry.getKey()).append(entry.getValue());
//        }
//
//        return "PreviousGtidsLogEvent{" +
//                "nsids=" + nsids +
//                ", gtidSetMap=" + sb.toString() +
//                '}';

        return "PreviousGtidsLogEvent{" +
                "mysqlGTIDSet=" + mysqlGTIDSet +
                '}';
    }
}

