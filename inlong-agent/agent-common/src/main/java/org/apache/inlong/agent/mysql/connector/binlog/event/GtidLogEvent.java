package org.apache.inlong.agent.mysql.connector.binlog.event;

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class GtidLogEvent extends LogEvent {

  /// Length of the commit_flag in event encoding
    public static final int ENCODED_FLAG_LENGTH= 1;
    /// Length of SID in event encoding
    public static final int ENCODED_SID_LENGTH                  =   16;
    public static final int LOGICAL_TIMESTAMP_TYPECODE_LENGTH   =   1;
    public static final int LOGICAL_TIMESTAMP_LENGTH            =   16;
    public static final int LOGICAL_TIMESTAMP_TYPECODE          =   2;

    public static final int [] BYTES_PER_SECTION                =   {4, 2, 2, 2, 6};

    private boolean     commitFlag;

    private UUID        sid;
    private long        gno;
    private long        lastCommitted = 0;
    private long        sequenceNumber = 0;

    public GtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
//        final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];

//        logger.info("GtidLogEvent LogBuffer hexdump: " + DBSyncUtils.bytes2string(buffer.getData()));

        buffer.position(commonHeaderLen);
        commitFlag = (buffer.getUint8() != 0); // ENCODED_FLAG_LENGTH
        
        //ignore gtid info read 
        // sid.copy_from((uchar *)ptr_buffer);
        // ptr_buffer+= ENCODED_SID_LENGTH;
        //
        // // SIDNO is only generated if needed, in get_sidno().
        // spec.gtid.sidno= -1;
        //
        // spec.gtid.gno= uint8korr(ptr_buffer);
        // ptr_buffer+= ENCODED_GNO_LENGTH;

        byte[] bytes = buffer.getData(ENCODED_SID_LENGTH);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        sid = new UUID(high, low);

//        buffer.forward(ENCODED_SID_LENGTH);
        gno = buffer.getLong64();

        if (buffer.hasRemaining() && buffer.remaining() > LOGICAL_TIMESTAMP_LENGTH &&
                buffer.getUint8() == LOGICAL_TIMESTAMP_TYPECODE) {
            lastCommitted = buffer.getLong64();
            sequenceNumber = buffer.getLong64();
        }
    }

    
    public boolean isCommitFlag() {
        return commitFlag;
    }

    public long getGno() {
        return gno;
    }

    public UUID getSid() {
        return sid;
    }

    public Long getLastCommitted() {
        return lastCommitted;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getGtidStr() {
        return sid.toString() + ":" + gno;
    }

    @Override
    public String toString() {
        return "GtidLogEvent{" +
                "commitFlag=" + commitFlag +
                ", sid=" + sid +
                ", gno=" + gno +
                ", lastCommitted=" + lastCommitted +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}
