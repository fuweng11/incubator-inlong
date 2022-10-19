/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.mysql.connector.driver;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.inlong.agent.mysql.connector.driver.packets.HeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.Reply323Packet;
import org.apache.inlong.agent.mysql.connector.driver.packets.client.AuthSwitchResponsePacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.client.ClientAuthenticationPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.AuthSwitchRequestMoreData;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.AuthSwitchRequestPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ErrorPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.HandshakeInitializationPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.MSC;
import org.apache.inlong.agent.mysql.connector.driver.utils.MySQLPasswordEncrypter;
import org.apache.inlong.agent.mysql.connector.driver.utils.PacketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MysqlConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlConnector.class);
    private InetSocketAddress address;
    private String username;
    private String password;

    private byte charsetNumber = 33;
    private String defaultSchema = "retl";
    private int soTimeout = 30 * 1000;
    private int receiveBufferSize = 16 * 1024;
    private int sendBufferSize = 16 * 1024;

    private SocketChannel channel;
    private AtomicBoolean connected = new AtomicBoolean(false);

    public MysqlConnector() {
    }

    public MysqlConnector(InetSocketAddress address, String username, String password) {

        this.address = address;
        this.username = username;
        this.password = password;
    }

    public MysqlConnector(InetSocketAddress address, String username, String password, byte charsetNumber,
            String defaultSchema) {
        this(address, username, password);

        this.charsetNumber = charsetNumber;
        this.defaultSchema = defaultSchema;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            channel = SocketChannel.open();
            try {
                configChannel(channel);
                LOGGER.info("connect MysqlConnection to {}...", address);
                channel.connect(address);
                LOGGER.info("MysqlConnection port : {}", channel.socket().getLocalPort());
                negotiate(channel);
            } catch (Exception e) {
                disconnect();
                connected.compareAndSet(true, false);
                throw new IOException("connect " + this.address + " failure:" + ExceptionUtils.getStackTrace(e));
            }
        } else {
            LOGGER.error("the channel can't be connected twice.");
        }
    }

    public void reconnect() throws IOException {
        disconnect();
        connect();
        LOGGER.info("reconnect use MysqlConnection Local port : {}", getLocalPort());
    }

    public void disconnect() throws IOException {
        if (connected.compareAndSet(true, false)) {
            try {
                if (channel != null) {
                    channel.close();
                }

                LOGGER.info("disConnect MysqlConnection to {}...", address);
            } catch (Exception e) {
                throw new IOException("disconnect " + this.address + " failure:" + ExceptionUtils.getStackTrace(e));
            }
        } else {
            LOGGER.info("the channel {} is not connected", this.address);
        }
    }

    public boolean isConnected() {
        return this.channel != null && this.channel.isConnected();
    }

    public MysqlConnector fork() {
        MysqlConnector connector = new MysqlConnector();
        connector.setCharsetNumber(getCharsetNumber());
        connector.setDefaultSchema(getDefaultSchema());
        connector.setAddress(getAddress());
        connector.setPassword(password);
        connector.setUsername(getUsername());
        connector.setReceiveBufferSize(getReceiveBufferSize());
        connector.setSendBufferSize(getSendBufferSize());
        connector.setSoTimeout(getSoTimeout());
        return connector;
    }

    // ====================== help method ====================

    private void configChannel(SocketChannel channel) throws IOException {
        channel.socket().setKeepAlive(true);
        channel.socket().setReuseAddress(true);
        channel.socket().setSoTimeout(soTimeout);
        channel.socket().setTcpNoDelay(true);
    }

    private void negotiate(SocketChannel channel) throws IOException {
        HeaderPacket header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        // check field_count
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket error = new ErrorPacket();
                error.fromBytes(body);
                throw new IOException("handshake exception:\n" + error.toString());
            } else if (body[0] == -2) {
                throw new IOException("Unexpected EOF packet at handshake phase.");
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
        HandshakeInitializationPacket handshakePacket = new HandshakeInitializationPacket();
        handshakePacket.fromBytes(body);
        if (handshakePacket.protocolVersion != MSC.DEFAULT_PROTOCOL_VERSION) {
            // HandshakeV9
            auth323(channel, (byte) (header.getPacketSequenceNumber() + 1), handshakePacket.seed);
            return;
        }

        LOGGER.info("handshake initialization packet received, prepare the client authentication packet to send");
        ClientAuthenticationPacket clientAuth = new ClientAuthenticationPacket();
        clientAuth.setCharsetNumber(charsetNumber);

        clientAuth.setUsername(username);
        clientAuth.setPassword(password);
        clientAuth.setServerCapabilities(handshakePacket.serverCapabilities);
        clientAuth.setScrumbleBuff(joinAndCreateScrumbleBuff(handshakePacket));
        clientAuth.setAuthPluginName("mysql_native_password".getBytes());

        byte[] clientAuthPkgBody = clientAuth.toBytes();
        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(clientAuthPkgBody.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));

        PacketManager.writePkg(channel, h.toBytes(), clientAuthPkgBody);
        LOGGER.info("client authentication packet is sent out.");

        // check auth result
        header = null;
        header = PacketManager.readHeader(channel, 4);
        body = null;
        body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        assert body != null;
        byte marker = body[0];
        if (marker == -2 || marker == 1) {
            byte[] authData = null;
            String pluginName = null;
            if (marker == 1) {
                AuthSwitchRequestMoreData packet = new AuthSwitchRequestMoreData();
                packet.fromBytes(body);
                authData = packet.authData;
            } else {
                AuthSwitchRequestPacket packet = new AuthSwitchRequestPacket();
                packet.fromBytes(body);
                authData = packet.authData;
                pluginName = packet.authName;
            }
            boolean isSha2Password = false;
            byte[] encryptedPassword = null;
            if (pluginName != null && "mysql_native_password".equals(pluginName)) {
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scramble411(password.getBytes(), authData);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            } else if (pluginName != null && "caching_sha2_password".equals(pluginName)) {
                isSha2Password = true;
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scrambleCachingSha2(password.getBytes(), authData);
                } catch (DigestException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            }
            assert encryptedPassword != null;
            AuthSwitchResponsePacket responsePacket = new AuthSwitchResponsePacket();
            responsePacket.authData = encryptedPassword;
            byte[] auth = responsePacket.toBytes();

            h = new HeaderPacket();
            h.setPacketBodyLength(auth.length);
            h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
            PacketManager.writePkg(channel, h.toBytes(), auth);
            LOGGER.info("auth switch response packet is sent out.");

            header = null;
            header = PacketManager.readHeader(channel, 4);
            body = null;
            body = PacketManager.readBytes(channel, header.getPacketBodyLength());
            assert body != null;
            if (isSha2Password) {
                if (body[0] == 0x01 && body[1] == 0x04) {
                    // password auth failed
                    throw new IOException("caching_sha2_password Auth failed");
                }

                header = null;
                header = PacketManager.readHeader(channel, 4);
                body = null;
                body = PacketManager.readBytes(channel, header.getPacketBodyLength());
            }
        }

        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
    }

    private void auth323(SocketChannel channel, byte packetSequenceNumber, byte[] seed) throws IOException {
        // auth 323
        Reply323Packet r323 = new Reply323Packet();
        if (password != null && password.length() > 0) {
            r323.seed = MySQLPasswordEncrypter.scramble323(password, new String(seed)).getBytes();
        }
        byte[] b323Body = r323.toBytes();

        HeaderPacket h323 = new HeaderPacket();
        h323.setPacketBodyLength(b323Body.length);
        h323.setPacketSequenceNumber((byte) (packetSequenceNumber + 1));

        PacketManager.writePkg(channel, h323.toBytes(), b323Body);
        LOGGER.info("client 323 authentication packet is sent out.");
        // check auth result
        HeaderPacket header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        assert body != null;
        switch (body[0]) {
            case 0:
                break;
            case -1:
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            default:
                throw new IOException("unpexpected packet with field_count=" + body[0]);
        }
    }

    private byte[] joinAndCreateScrumbleBuff(HandshakeInitializationPacket handshakePacket) throws IOException {
        byte[] dest = new byte[handshakePacket.seed.length + handshakePacket.restOfScrambleBuff.length];
        System.arraycopy(handshakePacket.seed, 0, dest, 0, handshakePacket.seed.length);
        System.arraycopy(handshakePacket.restOfScrambleBuff, 0, dest, handshakePacket.seed.length,
                handshakePacket.restOfScrambleBuff.length);
        return dest;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public byte getCharsetNumber() {
        return charsetNumber;
    }

    public void setCharsetNumber(byte charsetNumber) {
        this.charsetNumber = charsetNumber;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getLocalPort() {

        if (channel == null || channel.socket() == null) {
            return -1;
        }

        return channel.socket().getLocalPort();
    }

    public String getRemoteAddress() {
        if (connected.get()) {
            return channel.socket().getInetAddress().getHostAddress();
        }
        return null;
    }

}
