package org.apache.inlong.agent.plugin.dbsync.util;

import org.apache.inlong.agent.conf.DBSyncConf;

import java.util.concurrent.atomic.AtomicLong;

public class SequencialID {
    private AtomicLong id = new AtomicLong(1);
    private final long maxId = 100000000;
    private static SequencialID uniqueSequencialID = null;
    private String ip = null;

    public static final String SEQUENCIAL_ID = "sequencial_id";

    private SequencialID(String theIp) {
        ip = theIp;
    }

    public synchronized static SequencialID getInstance(
            DBSyncConf conf) {

        if (uniqueSequencialID == null) {
            uniqueSequencialID = new SequencialID(conf.getLocalIp());
        }
        return uniqueSequencialID;
    }

    public synchronized String getNextId() {
        if (id.get() > maxId) {
            id.set(1);
        }
        id.incrementAndGet();
        return ip + "#" + id.toString() + "#" + System.currentTimeMillis();
    }

}
