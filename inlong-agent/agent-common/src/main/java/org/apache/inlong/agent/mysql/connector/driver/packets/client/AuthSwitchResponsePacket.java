package org.apache.inlong.agent.mysql.connector.driver.packets.client;

import org.apache.inlong.agent.mysql.connector.driver.packets.CommandPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AuthSwitchResponsePacket extends CommandPacket {

    public byte[] authData;

    public void fromBytes(byte[] data) {
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(authData);
        return out.toByteArray();
    }

}
