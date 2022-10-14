package org.apache.inlong.agent.mysql.connector.driver.packets.server;

import org.apache.inlong.agent.mysql.connector.driver.packets.PacketWithHeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EOFPacket extends PacketWithHeaderPacket {

    public byte fieldCount;
    public int  warningCount;
    public int  statusFlag;

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                 Name
     *  -----                 ----
     *  1                     field_count, always = 0xfe
     *  2                     warning_count
     *  2                     Status Flags
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        fieldCount = data[index];
        index++;
        // 2. read warning count
        this.warningCount = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 3. read status flag
        this.statusFlag = ByteHelper.readUnsignedShortLittleEndian(data, index);
        // end read
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(5);
        out.write(this.fieldCount);
        ByteHelper.writeUnsignedShortLittleEndian(this.warningCount, out);
        ByteHelper.writeUnsignedShortLittleEndian(this.statusFlag, out);
        return out.toByteArray();
    }

}
