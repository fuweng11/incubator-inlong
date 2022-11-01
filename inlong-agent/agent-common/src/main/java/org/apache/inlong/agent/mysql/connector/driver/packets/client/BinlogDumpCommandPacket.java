package org.apache.inlong.agent.mysql.connector.driver.packets.client;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.mysql.connector.driver.packets.CommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * COM_BINLOG_DUMP
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class BinlogDumpCommandPacket extends CommandPacket {

    public long   binlogPosition;
    public long   slaveServerId;
    public String binlogFileName;

    public BinlogDumpCommandPacket(){
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
