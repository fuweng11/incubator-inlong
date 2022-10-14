package org.apache.inlong.agent.mysql.connector.driver.packets;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.inlong.agent.mysql.utils.CanalToStringStyle;

public abstract class CommandPacket implements IPacket {

    private byte command;

    // arg

    public void setCommand(byte command) {
        this.command = command;
    }

    public byte getCommand() {
        return command;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
