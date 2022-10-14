package org.apache.inlong.agent.mysql.relayLog;

import java.io.File;

import org.apache.commons.io.FileUtils;

import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;

public abstract class AbstractRelayLog implements RelayLog {

    protected final String relayPrefix;
    protected final String logPath;
    protected final long fileSize;
    protected final int blockSize;

    protected DBSyncConf config;

    public AbstractRelayLog(String relayPrefix, String logPath, DBSyncConf dbSyncConf){
        if (relayPrefix == null || logPath == null) {
            throw new IllegalArgumentException("log prefix & log root path can't be null");
        }

        this.config = dbSyncConf;
        this.logPath = logPath.endsWith("/") ? logPath : logPath + "/";
        checkDir(this.logPath);
        this.relayPrefix = relayPrefix;

        fileSize = config.getLongVar(ConfVars.RELAY_LOG_FILE_SIZE);
        blockSize = config.getIntVar(ConfVars.RELAY_LOG_BLOCK_SIZE);
    }

    protected void checkDir(final String path) {
        File dir = new File(path);
        FileUtils.deleteQuietly(dir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Create directory failed: " + dir.getAbsolutePath());
            }
        }
    }
}
