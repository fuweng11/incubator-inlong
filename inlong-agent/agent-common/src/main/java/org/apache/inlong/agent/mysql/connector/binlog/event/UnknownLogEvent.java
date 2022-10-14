package org.apache.inlong.agent.mysql.connector.binlog.event;

import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

/**
 * Unknown_log_event
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class UnknownLogEvent extends LogEvent
{
    public UnknownLogEvent(LogHeader header)
    {
        super(header);
    }
}
