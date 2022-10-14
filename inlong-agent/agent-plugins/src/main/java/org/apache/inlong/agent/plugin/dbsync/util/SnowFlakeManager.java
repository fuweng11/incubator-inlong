package org.apache.inlong.agent.plugin.dbsync.util;

import org.apache.inlong.agent.utils.SnowFlake;

import java.util.concurrent.ConcurrentHashMap;

public class SnowFlakeManager {

    /**
     * key: serverId, value: snowflake
     */
    private final ConcurrentHashMap<Long, SnowFlake> idGeneratorMap;

    public SnowFlakeManager() {
        idGeneratorMap = new ConcurrentHashMap<>();
    }

    /**
     * generate snow id using serverId;
     * @param serverId
     */
    public long generateSnowId(long serverId) {
        idGeneratorMap.computeIfAbsent(serverId, k -> new SnowFlake(serverId));
        return idGeneratorMap.get(serverId).nextId();
    }

}
