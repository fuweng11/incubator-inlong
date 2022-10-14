package org.apache.inlong.agent.plugin.dbsync.manager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.inlong.agent.plugin.dbsync.manager.db.DBSyncConfigDB;
import org.apache.inlong.agent.plugin.dbsync.manager.db.DBSyncDbEnv;
import org.apache.inlong.agent.plugin.dbsync.manager.db.DBSyncLogDB;
import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Description :
 * Author : lamberliu
 * Date : 2019/9/3
 */
public class DBManager {
    protected final Logger logger = LogManager.getLogger(this.getClass());

    private static final long DELAY_DEL_MILL_SEC = 2 * 60 * 60 * 1000;
    private final Map<String, MetaInfo> metaMap = new ConcurrentHashMap<>();

    public DBManager(String dbPath) throws IOException {
        init(dbPath);
    }

    private void init(String dbPath) throws IOException  {
        Files.list(Paths.get(dbPath)).filter(Files::isDirectory).forEach(path -> {
            try {
                DBSyncDbEnv dbEnv = new DBSyncDbEnv();
                dbEnv.setup(path.toFile(), false, false);
                DBSyncConfigDB configDB = new DBSyncConfigDB(dbEnv.getEnv());
                DBSyncLogDB logDB = new DBSyncLogDB(dbEnv.getEnv());

                MetaInfo meta = new MetaInfo(dbEnv, configDB, logDB);

                HashMap<String, JSONObject> configMap = configDB.walkAllDb();
                HashMap<String, JSONObject> logMap = logDB.walkAllDb();

                if (configMap.keySet().size() == 0 && logMap.keySet().size() == 0) {
                    logger.info("found a empty dir {}, so delete it.", path);
                    meta.close();
                    DBSyncUtils.delDirs(path.toAbsolutePath().toString());
                    return;
                }

                if (configMap.keySet().size() > 1 || logMap.keySet().size() > 1) {
                    // if configMap.size == 0 || logMap.size == 0 also will print below warning.
                    logger.warn("directory {} has multi-instance config or log", path.toAbsolutePath().toString());
                }

                // 如果一个目录下存在多个inst配置信息或记录信息，将多个inst的configDB和logDB均设为这个。
                Set<String> instNameSet = new HashSet<>();
                instNameSet.addAll(configMap.keySet());
                instNameSet.addAll(logMap.keySet());

                instNameSet.forEach(instName -> metaMap.put(instName, meta));
            } catch (Exception e) {
                logger.error("read bdb record {} error: ", path, e);
            }
        });
    }

    public synchronized void addMeta(String instName) {
        //add bdb environment
        String dirName = instName.replace('.', '-').replace(':', '-');
        String dirPrefix = DBSyncConf.getInstance(null).getDBPath();
        String fullName = dirPrefix.endsWith("/") ? (dirPrefix + dirName) : (dirPrefix + "/" + dirName);
        File dbDir = new File(fullName);

        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                logger.error("make directory " + fullName + " error");
            }
        }

        if (!metaMap.containsKey(instName)) {
            DBSyncDbEnv env = new DBSyncDbEnv();
            env.setup(dbDir, false, false);
            DBSyncConfigDB config = new DBSyncConfigDB(env.getEnv());
            DBSyncLogDB log = new DBSyncLogDB(env.getEnv());

            metaMap.put(instName, new MetaInfo(env, config, log));
            logger.warn("add a bdb env: {}", instName);
        } else {
            MetaInfo meta = metaMap.get(instName);
            meta.lastUpdTime = System.currentTimeMillis();
            meta.deleted = false;
            logger.warn("bdb env {} already exist", instName);
        }

    }

    public synchronized void rmMetaAndDir(String instName) {
        String dirName = instName.replace('.', '-').replace(':', '-');
        String dirPrefix = DBSyncConf.getInstance(null).getDBPath();
        String fullName = dirPrefix.endsWith("/") ? (dirPrefix + dirName) : (dirPrefix + "/" + dirName);

        if (metaMap.containsKey(instName)) {
            MetaInfo meta = metaMap.remove(instName);
//            meta.log.delConfig(instName);
//            meta.config.delConfig(instName);
            meta.close();
            DBSyncUtils.delDirs(fullName);
            logger.warn("deleted a bdb env: {}", instName);
        }
    }

    public synchronized void checkMeta(boolean force) {
        List<String> instList = new ArrayList<>();

        for (Map.Entry<String, MetaInfo> entry : metaMap.entrySet()) {
            String instName = entry.getKey();
            MetaInfo meta = entry.getValue();
            if (meta.deleted && (force || meta.lastUpdTime + DELAY_DEL_MILL_SEC < System.currentTimeMillis())) {
                instList.add(instName);
            }
        }

        if (!instList.isEmpty()) {
            logger.warn(": {}", Arrays.toString(instList.toArray()));
            instList.forEach(this::rmMetaAndDir);
        }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//        metaMap.entrySet().stream()
//                .filter( entry ->
//                        entry.getValue().deleted
//                                && entry.getValue().lastUpdTime + DELAY_DEL_MILL_SEC < System.currentTimeMillis())
//                .map(Map.Entry::getKey).forEach(this::rmMetaAndDir);
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//        metaMap.entrySet().(entry ->
//                entry.getValue().deleted &&
//                        entry.getValue().lastUpdTime + DELAY_DEL_MILL_SEC < System.currentTimeMillis());
    }

    public JSONObject getLog(String instName) throws Exception {
//        MetaInfo meta = metaMap.get(instName);
//        if (meta == null) {
//            throw new NoSuchElementException("not found element " + instName);
//        }
//
//        meta.lastUpdTime = System.currentTimeMillis();
//        return meta.log.getLog(instName);
        return null;
    }

    @VisibleForTesting
    HashMap<String, JSONObject> getConfig(String instName) throws Exception {
        MetaInfo meta = metaMap.get(instName);
        if (meta == null) {
            throw new NoSuchElementException("not found element " + instName);
        }

        return meta.config.walkAllDb();
    }

    public void putConfig(String instName, JSONObject config) throws Exception {
        MetaInfo meta = metaMap.get(instName);
        if (meta == null) {
            throw new NoSuchElementException("not found element " + instName);
        }

        meta.lastUpdTime = System.currentTimeMillis();
        meta.config.putConfig(instName, config);
        meta.config.flush();
    }

    public void putLog(String instName, JSONObject log) throws Exception {
        MetaInfo meta = metaMap.get(instName);
        if (meta == null) {
            throw new NoSuchElementException("not found element " + instName);
        }

        meta.lastUpdTime = System.currentTimeMillis();
        meta.log.putLog(instName, log);
        meta.log.flush();
    }

    public Map<String, JSONObject> walkLogs() throws Exception {
        HashMap<String, JSONObject> logList = new HashMap<>();

        for (Map.Entry<String, MetaInfo> entry : metaMap.entrySet()) {
//            String instName = entry.getKey();
            MetaInfo meta = entry.getValue();
            logList.putAll(meta.log.walkAllDb());
        }

        return logList;
    }

    public Map<String, JSONObject> walkConfigs() throws Exception {
        HashMap<String, JSONObject> configList = new HashMap<>();

        for (Map.Entry<String, MetaInfo> entry : metaMap.entrySet()) {
//            String instName = entry.getKey();
            MetaInfo meta = entry.getValue();
            configList.putAll(meta.config.walkAllDb());
        }

        return configList;
    }


    public MetaInfo getMeta(String instName) {
        return metaMap.get(instName);
    }

    public void delMeta(String instName) {
        if (metaMap.containsKey(instName)) {
            metaMap.get(instName).deleted = true;
        }
    }

    public void close() {
        metaMap.forEach( (s, m) -> {
            m.close();
        });

        metaMap.clear();
    }

    @VisibleForTesting
    Map<String, MetaInfo> getMetaMap() {
        return metaMap;
    }

    public void reportConfig() {
        try {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("\nbdbConfig list:\n");
            walkConfigs().forEach((key, value) -> metaMap.compute(key, (k, v) -> {
                if (v == null) {
                    logger.error("{} config in bdb but not in metaMap", key);
                } else {
                    stringBuffer.append(key).append("==>")
                            .append("deleted:").append(v.deleted)
                            .append(":").append(value.toString())
                            .append("\n");
                }
                return v;
            }));
            logger.info(stringBuffer.toString());
        } catch (Exception e) {
            logger.error("DBManager report config exception: ", e);
        }
    }

    public void reportLog() {
        try {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("\nbdbLog list:\n");
            walkLogs().forEach((key, value) -> metaMap.compute(key, (k, v) -> {
                if (v == null) {
                    logger.error("{} log in bdb but not in metaMap", key);
                } else {
                    stringBuffer.append(key).append("==>")
                            .append("deleted:").append(v.deleted)
                            .append(":").append(value.toString())
                            .append("\n");
                }
                return v;
            }));
            logger.info(stringBuffer.toString());
        } catch (Exception e) {
            logger.error("DBManager report Log exception: ", e);
        }
    }

    public static class MetaInfo {
        private DBSyncDbEnv env;
        private DBSyncConfigDB config;
        private DBSyncLogDB log;
        private long lastUpdTime;
        private boolean deleted;

        public MetaInfo(DBSyncDbEnv env, DBSyncConfigDB config, DBSyncLogDB log) {
            this(env, config, log, System.currentTimeMillis());
        }

        public MetaInfo(DBSyncDbEnv env, DBSyncConfigDB config, DBSyncLogDB log, long time) {
            this(env, config, log, time, false);
        }

        public MetaInfo(DBSyncDbEnv env, DBSyncConfigDB config, DBSyncLogDB log, long time, boolean deleted) {
            this.env = env;
            this.config = config;
            this.log = log;
            this.lastUpdTime = time;
            this.deleted = deleted;
        }

        public void close() {
            if (log != null) {
                log.close();
            }
            if (config != null) {
                config.close();
            }
            if (env != null) {
                env.close();
            }
        }
    }
}
