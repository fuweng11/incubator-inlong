package org.apache.inlong.agent.mysql.connector.driver;

import org.apache.inlong.agent.mysql.connector.driver.packets.client.QueryCommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ErrorPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.OKPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.PacketManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class MysqlUpdateExecutor {

    private static final Logger logger = LogManager.getLogger(MysqlUpdateExecutor.class);

    private SocketChannel       channel;

    public MysqlUpdateExecutor(MysqlConnector connector){
        if (!connector.isConnected()) {
            throw new RuntimeException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public MysqlUpdateExecutor(SocketChannel ch){
        this.channel = ch;
    }

    public OKPacket update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);

        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(channel, PacketManager.readHeader(channel, 4).getPacketBodyLength());
        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + updateString);
        }

        OKPacket packet = new OKPacket();
        packet.fromBytes(body);
        return packet;
    }
}
