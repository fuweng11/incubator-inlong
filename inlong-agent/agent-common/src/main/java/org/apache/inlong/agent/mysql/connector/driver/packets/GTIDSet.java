package org.apache.inlong.agent.mysql.connector.driver.packets;

import java.io.IOException;

public interface GTIDSet {

    /**
     * 序列化成字节数组
     *
     * @return
     */
    byte[] encode() throws IOException;

    /**
     * 更新当前实例
     * 
     * @param str
     * @throws Exception
     */
    void update(String str);
}
