package org.apache.inlong.agent.core.ha.lb;

/**
 * 功能描述：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 14:49
 * @Description:
 */
public interface DbSyncHostUsage {
    /**
     * Returns the host usage information.
     *
     * @return Broker host usage in the json string format
     */
    SystemResourceUsage getDbSyncHostUsage();

    /**
     * Calculate the host usage information.
     */
    void calculateDbSyncHostUsage();
}