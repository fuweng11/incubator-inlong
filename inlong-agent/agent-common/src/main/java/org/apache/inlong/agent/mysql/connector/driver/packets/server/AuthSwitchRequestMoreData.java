package org.apache.inlong.agent.mysql.connector.driver.packets.server;

import org.apache.inlong.agent.mysql.connector.driver.packets.CommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import java.io.IOException;

public class AuthSwitchRequestMoreData extends CommandPacket {

    public int    status;
    public byte[] authData;

    @Override
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }


}
