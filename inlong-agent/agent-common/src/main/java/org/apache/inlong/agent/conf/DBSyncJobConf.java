/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.conf;

import org.apache.inlong.agent.mysql.filter.CanalEventFilter;
import org.apache.inlong.agent.mysql.filter.exception.CanalFilterException;
import org.apache.inlong.agent.mysql.protocol.position.LogIdentity;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.TaskStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.LRUMap;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DBSyncJobConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncJobConf.class);

    /*
     * job 采集状态控制
     */
    /*
     * 正常采集
     */
    public static final Integer RUNNING_MODEL_NORMAL = 1;

    /*
     * 暂停采集
     */
    public static final Integer RUNNING_MODEL_PAUSE = 2;

    /*
     * 拉取数据，但是全部过滤掉，即不发送到pulsar，也不发送指标
     */
    public static final Integer RUNNING_MODEL_FILTER = 3;

    /*
     * 停止的任务
     */
    public static final Integer RUNNING_MODEL_STOPPED = 4;

    /*
     * 任务状态配置方式
     */
    public static final Integer HANDLE_TYPE_AUTO = 0;

    public static final Integer HANDLE_TYPE_MANUAL = 1;

    private static final AtomicReferenceFieldUpdater<DBSyncJobConf, TaskStat> STATUS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DBSyncJobConf.class, TaskStat.class, "status");

    private static final AtomicReferenceFieldUpdater<DBSyncJobConf, DbAddConfigInfo> ADDR_UPDATE =
            AtomicReferenceFieldUpdater.newUpdater(DBSyncJobConf.class, DbAddConfigInfo.class, "currConfigAddress");
    private final AtomicInteger dbIndex = new AtomicInteger(0);
    private String mysqlUserName;
    private String mysqlPassWd;
    private Charset charset;
    private int maxUnAckedLogPositions = 100000;
    private List<DbAddConfigInfo> dbConfigAddrList;
    private volatile JobStat.TaskStat status;
    private volatile DbAddConfigInfo currConfigAddress;
    private ConcurrentHashMap<String, Pattern> namePatternMap;
    private ConcurrentHashMap<String, List<MysqlTableConf>> table2MysqlConf;
    private LRUMap<String, CopyOnWriteArrayList<String>> lruCache;
    private TableNameFilter filter;

    private LogPosition startPos;
    private String dbJobId;
    private List<MQClusterInfo> mqClusterInfos = new ArrayList<>();

    public DBSyncJobConf(String add, int port, String bakDbIp, int bakDbPort,
            String userName, String passwd, Charset charset, LogPosition startPos, String dbJobId) {
        this.currConfigAddress = initDbAddressList(add, port, bakDbIp, bakDbPort, startPos);
        this.mysqlUserName = userName;
        this.mysqlPassWd = passwd;
        this.charset = charset;
        this.startPos = startPos;
        this.dbJobId = dbJobId;
        this.namePatternMap = new ConcurrentHashMap<>();
        this.table2MysqlConf = new ConcurrentHashMap<>();
        this.lruCache = new LRUMap<>(24 * 60 * 60 * 1000L);
        this.filter = new TableNameFilter();
        STATUS_UPDATER.set(DBSyncJobConf.this, TaskStat.NORMAL);
    }

    private void addMysqlTableConf(String key, Pattern pattern, MysqlTableConf mysqlTableConf) {
        namePatternMap.put(key, pattern);
        table2MysqlConf.compute(key, (k, v) -> {
            if (v == null) {
                v = Lists.newArrayList();
            }
            v.add(mysqlTableConf);
            return v;
        });
    }

    public void updateUserPasswd(String userName, String passwd) {
        this.mysqlUserName = userName;
        this.mysqlPassWd = passwd;
    }

    public boolean bInNeedTable(String dbName, String tbName) {
        String fullName = dbName + "." + tbName;
        return lruCache.containsKey(fullName) || namePatternMap.containsKey(fullName);
    }

    public void addTable(MysqlTableConf tableConf) {
        String namePatternStr = tableConf.getDataBaseName() + "." + tableConf.getTableName();
        Pattern pattern = Pattern.compile(namePatternStr, Pattern.CASE_INSENSITIVE);
        addMysqlTableConf(namePatternStr, pattern, tableConf);
    }

    public MysqlTableConf getMysqlTableConf(String dbName, String tbName) {
        List<MysqlTableConf> confList = getMysqlTableConfList(dbName, tbName);
        if (confList != null && !confList.isEmpty()) {
            return confList.get(0);
        } else {
            return null;
        }
    }

    public synchronized void updateMqClusterInfos(List<MQClusterInfo> newMqClusterInfos) {
        if (newMqClusterInfos == null || newMqClusterInfos.size() == 0) {
            return;
        }
        if (mqClusterInfos != null && mqClusterInfos.size() == 0) {
            mqClusterInfos.addAll(newMqClusterInfos);
        } else if (compareMqClusterInfoForEqual(mqClusterInfos, newMqClusterInfos)) {
            mqClusterInfos.clear();
            mqClusterInfos.addAll(newMqClusterInfos);
        }
    }

    private boolean compareMqClusterInfoForEqual(List<MQClusterInfo> oldMqClusterInfos,
            List<MQClusterInfo> newMqClusterInfos) {
        boolean result = true;
        if (oldMqClusterInfos.size() != newMqClusterInfos.size()) {
            result = false;
        }
        for (MQClusterInfo newMqClusterInfo : newMqClusterInfos) {
            for (MQClusterInfo oldMqClusterInfo : oldMqClusterInfos) {
                if (newMqClusterInfo.compareTo(oldMqClusterInfo) != 0) {
                    result = false;
                    break;
                }
            }
            if (!result) {
                break;
            }
        }
        return result;
    }

    public synchronized List<MQClusterInfo> getMqClusterInfos() {
        if (mqClusterInfos != null && mqClusterInfos.size() > 0) {
            return mqClusterInfos;
        } else {
            Collection<MysqlTableConf> col = getMysqlTableConfList();
            if (col != null && col.size() > 0) {
                for (MysqlTableConf mysqlTableConf : col) {
                    List<MQClusterInfo> list = mysqlTableConf.getTaskInfo().getMqClusters();
                    if (list != null && list.size() > 0) {
                        mqClusterInfos = list;
                    }
                }
            }
        }
        return mqClusterInfos;
    }

    public MysqlTableConf getMysqlTableConf(Integer taskId) {
        Optional<MysqlTableConf> optionalMysqlTableConf = table2MysqlConf
                .values()
                .stream()
                .flatMap(List::stream)
                .filter(tableConf -> Objects.equals(tableConf.getTaskId(), taskId))
                .findAny();

        return optionalMysqlTableConf.orElse(null);
    }

    public List<MysqlTableConf> getMysqlTableConfList(String dbName, String tbName) {
        String fullName = dbName + "." + tbName;
        CopyOnWriteArrayList<String> namePatterns = lruCache.get(fullName);
        List<MysqlTableConf> confList = new ArrayList<>();
        if (namePatterns != null) {
            for (String namePattern : namePatterns) {
                confList.addAll(table2MysqlConf.get(namePattern));
            }
        } else {
            confList = table2MysqlConf.get(fullName);
        }

        if (confList != null) {
            return ImmutableList.copyOf(confList);
        } else {
            return null;
        }
    }

    public Collection<MysqlTableConf> getMysqlTableConfList() {
        return table2MysqlConf.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void removeTable(Integer taskId) {
        table2MysqlConf.values()
                .forEach(confList -> confList.removeIf(conf -> Objects.equals(conf.getTaskId(), taskId)));

        table2MysqlConf.entrySet().removeIf(entry -> {
            List<MysqlTableConf> confList = entry.getValue();
            if (confList != null && confList.isEmpty()) {
                String fullName = entry.getKey();
                namePatternMap.remove(fullName);
                lruCache.removeValue(new CopyOnWriteArrayList<>(Collections.singletonList(fullName)));
                return true;
            } else {
                return false;
            }
        });
    }

    private DbAddConfigInfo initDbAddressList(String masterAddr, int port,
            String bakAddr, int bakDbPort, LogPosition startPos) {
        this.dbConfigAddrList = Lists.newArrayList(new DbAddConfigInfo(masterAddr, port));
        if (bakAddr != null && bakDbPort != -1) {
            this.setBakMysqlInfo(bakAddr, bakDbPort);
        }
        DbAddConfigInfo currentDbAddInfo = null;
        if (startPos != null && startPos.getIdentity() != null) {
            LogIdentity logIdentity = startPos.getIdentity();
            if (logIdentity.getSourceAddress() != null) {
                String lastIp = logIdentity.getSourceAddress().getAddress().getHostAddress();
                int lastPort = logIdentity.getSourceAddress().getPort();

                for (DbAddConfigInfo dbAddConfigInfo : dbConfigAddrList) {
                    if (dbAddConfigInfo.getRealRemoteIp().equals(lastIp)
                            && dbAddConfigInfo.getPort() == lastPort) {
                        currentDbAddInfo = dbAddConfigInfo;
                        break;
                    }
                }
            }
        }
        if (currentDbAddInfo == null) {
            currentDbAddInfo = dbConfigAddrList.get(0);
        }
        return currentDbAddInfo;
    }

    public boolean bNoNeedDb() {
        return namePatternMap.isEmpty() && table2MysqlConf.isEmpty();
    }

    public boolean containsDatabase(String host, int port) {
        DbAddConfigInfo address = new DbAddConfigInfo(host, port);
        return dbConfigAddrList.contains(address);
    }

    public boolean containsDatabase(String url) {
        if (StringUtils.isNotEmpty(url)) {
            String[] str = url.split(":");
            if (str != null && str.length > 1) {
                try {
                    DbAddConfigInfo address = new DbAddConfigInfo(str[0], Integer.parseInt(str[1]));
                    return dbConfigAddrList.contains(address);
                } catch (Exception e) {
                    LOGGER.error("Data base url has error!", e);
                }
            }
        }
        return false;
    }

    public boolean containsTask(Integer taskId) {
        return table2MysqlConf.values().stream()
                .flatMap(List::stream)
                .anyMatch(tableConf -> Objects.equals(tableConf.getTaskId(), taskId));
    }

    public List<Integer> getTaskIdList() {
        return table2MysqlConf.values().stream()
                .flatMap(List::stream)
                .map(MysqlTableConf::getTaskId)
                .collect(Collectors.toList());
    }

    public DbAddConfigInfo getCurMysqlAddress() {
        if (currConfigAddress != null) {
            return currConfigAddress;
        }
        return null;
    }

    public String getCurMysqlIp() {
        if (currConfigAddress != null) {
            return currConfigAddress.getRealRemoteIp();
        }
        return null;
    }

    public String getCurMysqlUrl() {
        if (currConfigAddress != null) {
            return currConfigAddress.getDbAddress() + ":" + currConfigAddress.getPort();
        }
        return "";
    }

    public int getCurMysqlPort() {
        if (currConfigAddress != null) {
            return currConfigAddress.getPort();
        }
        return -1;
    }

    public String getMysqlUserName() {
        return mysqlUserName;
    }

    public String getMysqlPassWd() {
        return mysqlPassWd;
    }

    public String getDbJobId() {
        return dbJobId;
    }

    public Charset getCharset() {
        return charset;
    }

    public void updateCharset(Charset charset) {
        this.charset = charset;
    }

    public void updateCharset(Charset charset, Integer taskId) {
        Optional<MysqlTableConf> optionalMysqlTableConf = table2MysqlConf
                .values()
                .stream()
                .flatMap(List::stream)
                .filter(tableConf -> Objects.equals(tableConf.getTaskId(), taskId))
                .findAny();

        optionalMysqlTableConf.ifPresent(conf -> conf.setCharset(charset));
    }

    public int getMaxUnAckedLogPositions() {
        return maxUnAckedLogPositions;
    }

    public void setMaxUnAckedLogPositions(int maxUnAckedLogPositions) {
        this.maxUnAckedLogPositions = maxUnAckedLogPositions;
    }

    public CanalEventFilter<String> getFilter() {
        return filter;
    }

    public JobStat.TaskStat getStatus() {
        return status;
    }

    public void setStatus(TaskStat stat) {
        STATUS_UPDATER.set(DBSyncJobConf.this, stat);
    }

    public DbAddConfigInfo getMstMysqlAddr() {
        if (dbConfigAddrList.size() > 0) {
            return dbConfigAddrList.get(0);
        }
        return null;
    }

    public synchronized DbAddConfigInfo getNextMysqlAddr() {
        int index = (dbIndex.get() + 1) % dbConfigAddrList.size();
        return dbConfigAddrList.get(index);
    }

    public String getBakMysqlUrl() {
        if (dbConfigAddrList.size() > 1) {
            return dbConfigAddrList.get(1).getDbAddress() + ":" + dbConfigAddrList.get(1).getPort();
        }
        return null;
    }

    private void validateAddr(String host, int port) throws IllegalArgumentException {
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("host name " + host + " is null or empty");
        }

        if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + port);
        }

    }

    public void setBakMysqlInfo(String host, int port) {
        validateAddr(host, port);
        this.dbConfigAddrList.add(new DbAddConfigInfo(host, port));
    }

    public LogPosition getStartPos() {
        return startPos;
    }

    public void resetDbInfo(String url, String bakUrl) {
        String mysqlAdd = DBSyncUtils.getHost(url);
        int mysqlPort = DBSyncUtils.getPort(url);

        List<DbAddConfigInfo> newDbAddressList = Lists.newArrayList();
        newDbAddressList.add(new DbAddConfigInfo(mysqlAdd, mysqlPort));

        // if bakUrl is valid, add it to newDbAddressList
        String bakMysqlAdd = DBSyncUtils.getHost(bakUrl);
        int bakMysqlPort = DBSyncUtils.getPort(bakUrl);
        if (StringUtils.isBlank(bakMysqlAdd) || bakMysqlPort <= 0) {
            LOGGER.warn("bakMysqlAddr {} is empty or bakMysqlPort {} <=0, set to empty", bakMysqlAdd, bakMysqlPort);
        } else {
            newDbAddressList.add(new DbAddConfigInfo(bakMysqlAdd, bakMysqlPort));
        }
        dbConfigAddrList = newDbAddressList;
    }

    public void doReset() throws IllegalStateException {
        if (status == TaskStat.SWITCHING) {
            throw new IllegalStateException("current job is switching");
        }

        dbIndex.set(0);
        ADDR_UPDATE.set(DBSyncJobConf.this, dbConfigAddrList.get(dbIndex.get()));
        STATUS_UPDATER.set(DBSyncJobConf.this, TaskStat.NORMAL);
    }

    public void doSwitch() throws IllegalStateException {
        if (status == TaskStat.SWITCHING || status == TaskStat.RESETTING) {
            throw new IllegalStateException("current job is switching or resetting");
        }

        int index = dbIndex.incrementAndGet() % dbConfigAddrList.size();
        ADDR_UPDATE.set(DBSyncJobConf.this, dbConfigAddrList.get(index));
        STATUS_UPDATER.set(DBSyncJobConf.this, TaskStat.SWITCHING);
    }

    public boolean bSwitched() {
        return status == JobStat.TaskStat.SWITCHING
                || status == JobStat.TaskStat.SWITCHED;
    }

    @Override
    public String toString() {
        return "DBSyncJobConf{mysqlUserName='" + mysqlUserName + '\'' + ", mysqlPassWd='" + mysqlPassWd + '\''
                + ", charset=" + charset + ", dbAddrList=" + dbConfigAddrList + ", startPos=" + startPos + '\'' + '}';
    }

    public List<DbAddConfigInfo> getDbAddrList() {
        return dbConfigAddrList;
    }

    public void setDbAddrList(List<DbAddConfigInfo> dbConfigAddrList) {
        this.dbConfigAddrList = dbConfigAddrList;
    }

    private class TableNameFilter implements CanalEventFilter<String> {

        @Override
        public boolean filter(final String event) throws CanalFilterException {

            boolean bFind = false;
            CopyOnWriteArrayList<String> findEvents = new CopyOnWriteArrayList<>();

            boolean lruCacheFound = false;
            if (lruCache.containsKey(event)) {
                // confirm get table conf
                List<String> pattern = lruCache.get(event);
                lruCacheFound = pattern.stream().anyMatch(table2MysqlConf::containsKey);
            }
            for (Map.Entry<String, Pattern> e : namePatternMap.entrySet()) {
                if (e.getValue().matcher(event).matches()) {
                    bFind = true;
                    findEvents.add(e.getKey());
                }
                if (bFind) {
                    lruCache.put(event, findEvents);
                }
            }
            return lruCacheFound || bFind;
        }
    }

    public static String getIpByHostname(String mysqlAddress) {
        String ip = null;
        try {
            InetAddress[] addresses = InetAddress.getAllByName(mysqlAddress);
            if (addresses != null && addresses.length > 0) {
                int index = (Math.abs((int) Instant.now().toEpochMilli())) % addresses.length;
                ip = addresses[index].getHostAddress();
            }
        } catch (UnknownHostException e) {
            LOGGER.error("getIpByHostname has Exception url {}, ip is null", mysqlAddress, e);
        }
        return ip;
    }
}
