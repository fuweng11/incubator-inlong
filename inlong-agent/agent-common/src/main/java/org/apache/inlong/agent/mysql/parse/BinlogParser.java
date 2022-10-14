package org.apache.inlong.agent.mysql.parse;

import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.utils.CanalLifeCycle;

/**
 * 解析binlog的接口
 *
 * @author: yuanzu Date: 12-9-20 Time: 下午8:46
 */
public interface BinlogParser<T> extends CanalLifeCycle {

    org.apache.inlong.agent.common.protocol.CanalEntry.Entry parse(T event, DBSyncJobConf jobconf) throws CanalParseException;

    void reset(boolean bCleanTableMap);
}
