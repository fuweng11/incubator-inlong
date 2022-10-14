package org.apache.inlong.agent.mysql.relayLog;

public interface RelayLog {
    public static final byte[] magicBytes = new byte[] { 0x01, 0x05, (byte) 0xBA, (byte) 0xDB };
    public boolean putLog(byte[] bytes);
    public byte[] getLog();
    public void close();
    public boolean isEmpty();
    public void clearLog();
    public void report();
}
