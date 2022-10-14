package org.apache.inlong.agent.mysql.connector.driver.packets.client;

import org.apache.inlong.agent.mysql.connector.driver.packets.CommandPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class QueryCommandPacket extends CommandPacket {

    private String queryString;

    public QueryCommandPacket(){
        setCommand((byte) 0x03);
    }

    public void fromBytes(byte[] data) throws IOException {
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        out.write(getQueryString().getBytes());
        return out.toByteArray();
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getQueryString() {
        return queryString;
    }

}
