package org.apache.inlong.agent.mysql.connector.driver;

import org.apache.inlong.agent.mysql.connector.driver.packets.HeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.client.QueryCommandPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ErrorPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.FieldPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetHeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.RowDataPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.PacketManager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class MysqlQueryExecutor {

    private SocketChannel channel;

    public MysqlQueryExecutor(MysqlConnector connector){
        if (!connector.isConnected()) {
            throw new RuntimeException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public MysqlQueryExecutor(SocketChannel ch){
        this.channel = ch;
    }

    /**
     * (Result Set Header Packet) the number of columns <br>
     * (Field Packets) column descriptors <br>
     * (EOF Packet) marker: end of Field Packets <br>
     * (Row Data Packets) row contents <br>
     * (EOF Packet) marker: end of Data Packets
     * 
     * @param queryString
     * @return
     * @throws IOException
     */
    public ResultSetPacket query(String queryString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(queryString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);
        byte[] body = readNextPacket();

        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + queryString);
        }

        ResultSetHeaderPacket rsHeader = new ResultSetHeaderPacket();
        rsHeader.fromBytes(body);

        List<FieldPacket> fields = new ArrayList<FieldPacket>();
        for (int i = 0; i < rsHeader.getColumnCount(); i++) {
            FieldPacket fp = new FieldPacket();
            fp.fromBytes(readNextPacket());
            fields.add(fp);
        }

        readEofPacket();

        List<RowDataPacket> rowData = new ArrayList<RowDataPacket>();
        while (true) {
            body = readNextPacket();
            if (body[0] == -2) {
                break;
            }
            RowDataPacket rowDataPacket = new RowDataPacket();
            rowDataPacket.fromBytes(body);
            rowData.add(rowDataPacket);
        }

        ResultSetPacket resultSet = new ResultSetPacket();
        resultSet.getFieldDescriptors().addAll(fields);
        for (RowDataPacket r : rowData) {
            resultSet.getFieldValues().addAll(r.getColumns());
        }
        resultSet.setSourceAddress(channel.socket().getRemoteSocketAddress());

        return resultSet;
    }

    private void readEofPacket() throws IOException {
        byte[] eofBody = readNextPacket();
        if (eofBody[0] != -2) {
            throw new IOException("EOF Packet is expected, but packet with field_count=" + eofBody[0] + " is found.");
        }
    }

    protected byte[] readNextPacket() throws IOException {
        HeaderPacket h = PacketManager.readHeader(channel, 4);
        return PacketManager.readBytes(channel, h.getPacketBodyLength());
    }
}
