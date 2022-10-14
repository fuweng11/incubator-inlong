package org.apache.inlong.agent.mysql.protocol.position;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import org.apache.inlong.agent.mysql.utils.CanalToStringStyle;

/**
 * 事件唯一标示
 */
public abstract class Position implements Serializable {

    private static final long serialVersionUID = 2332798099928474975L;

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
