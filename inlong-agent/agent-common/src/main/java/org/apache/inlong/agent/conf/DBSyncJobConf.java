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
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.TaskStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.JsonUtils.JSONArray;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.LRUMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

    public static final String JOB_MYSQL_IP = "job.mysql.ip";
    public static final String JOB_MYSQL_PORT = "job.mysql.port";
    public static final String JOB_MYSQL_USERNAME = "job.mysql.username";
    public static final String JOB_MYSQL_PASSWD = "job.mysql.passwd";
    public static final String INST_NAME = "inst.name";
    public static final String JOB_STATUS = "job.status";
    public static final String JOB_SEP = "job.sep";
    public static final String JOB_CHARSET_NAME = "job.charset.name";
    public static final String JOB_BAK_MYSQL_IP = "job.bak.mysql.ip";
    public static final String JOB_BAK_MYSQL_PORT = "job.bak.mysql.port";
    public static final String JOB_TABLES = "job.tables";
    public static final String JOB_TABLE_LIST = "job.table.list";
    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncJobConf.class);
    private static final AtomicReferenceFieldUpdater<DBSyncJobConf, TaskStat> STATUS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DBSyncJobConf.class, TaskStat.class, "status");
    private static final AtomicReferenceFieldUpdater<DBSyncJobConf, InetSocketAddress> ADDR_UPDATE =
            AtomicReferenceFieldUpdater.newUpdater(DBSyncJobConf.class, InetSocketAddress.class, "currAddress");
    private final AtomicInteger dbIndex = new AtomicInteger(0);
    private String mysqlUserName;
    private String mysqlPassWd;
    private Charset charset;
    private int maxUnackedLogPositions = 100000;
    private List<InetSocketAddress> dbAddrList;
    private volatile JobStat.TaskStat status;
    private volatile InetSocketAddress currAddress;
    private ConcurrentHashMap<String, Pattern> namePatternMap;
    private ConcurrentHashMap<String, List<MysqlTableConf>> table2MysqlConf;
    private LRUMap<String, CopyOnWriteArrayList<String>> lruCache;
    private TableNameFilter filter;
    private HashSet<String> jobAlarmPerson;

    private LogPosition startPos;
    private String jobName;
    private String serverName;

    public DBSyncJobConf(String ip, int port, String userName, String passwd, Charset charset, LogPosition startPos,
            String serverName) {
        this.dbAddrList = Lists.newArrayList(new InetSocketAddress(ip, port));
        this.currAddress = new InetSocketAddress(ip, port);
        this.mysqlUserName = userName;
        this.mysqlPassWd = passwd;
        this.charset = charset;
        this.startPos = startPos;
        this.jobName = currAddress.getHostString() + ":" + currAddress.getPort() + ":" + serverName;
        this.namePatternMap = new ConcurrentHashMap<>();
        this.table2MysqlConf = new ConcurrentHashMap<>();
        this.lruCache = new LRUMap<>(24 * 60 * 60 * 1000L);
        this.jobAlarmPerson = new HashSet<>();
        this.filter = new TableNameFilter();
        this.serverName = serverName;
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

    private void putDbInfo(JSONObject info) throws IllegalStateException {
        if (dbAddrList.isEmpty()) {
            throw new IllegalStateException("DB info is empty");
        } else {
            info.put(JOB_MYSQL_IP, dbAddrList.get(0).getHostString());
            info.put(JOB_MYSQL_PORT, dbAddrList.get(0).getPort());
        }

        if (dbAddrList.size() > 1) {
            info.put(JOB_BAK_MYSQL_IP, dbAddrList.get(1).getHostString());
            info.put(JOB_BAK_MYSQL_PORT, dbAddrList.get(1).getPort());
        }
    }

    public JSONObject getJsonObj() {
        JSONObject obj = new JSONObject();
        obj.put(INST_NAME, getJobName());
        obj.put(JOB_MYSQL_USERNAME, this.mysqlUserName);
        obj.put(JOB_MYSQL_PASSWD, this.mysqlPassWd);
        obj.put(JOB_STATUS, this.status.name());
        obj.put(JOB_CHARSET_NAME, charset.name());

        JSONObject tableJson = new JSONObject();
        table2MysqlConf.forEach((key, value) -> {
            List<JSONObject> tableInfoList = value.stream()
                    .map(MysqlTableConf::getJsonObj)
                    .collect(Collectors.toList());
            JSONArray tbInfoJSONArr = new JSONArray();
            tableInfoList.forEach(tbInfoJSONArr::add);
            tableJson.put(key, tbInfoJSONArr);
        });
        obj.put(JOB_TABLE_LIST, tableJson);

        // for compatibility
        JSONObject comTableJson = new JSONObject();
        table2MysqlConf.forEach((key, value) -> {
            if (!value.isEmpty()) {
                JSONObject tbInfoJson = value.get(0).getJsonObj();
                comTableJson.put(key, tbInfoJson);
            }
        });
        obj.put(JOB_TABLES, comTableJson);

        putDbInfo(obj);

        return obj;
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

    public void removeTable(String dbName, String tbName) {
        String fullName = dbName + "." + tbName;
        namePatternMap.remove(fullName);
        table2MysqlConf.remove(fullName);
        lruCache.removeValue(new CopyOnWriteArrayList<>(Collections.singletonList(fullName)));
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

    public boolean bNoNeedDb() {
        return namePatternMap.isEmpty() && table2MysqlConf.isEmpty();
    }

    public boolean containsDatabase(String url) {
        InetSocketAddress address = new InetSocketAddress(DBSyncUtils.getHost(url), DBSyncUtils.getPort(url));
        return dbAddrList.contains(address);
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

    public JSONArray getTasksJson() {
        JSONArray taskIds = new JSONArray();
        HashSet<Integer> tmpIdSet = new HashSet<>(getTaskIdList());
        for (Integer taskId : tmpIdSet) {
            taskIds.add(taskId);
        }
        return taskIds;
    }

    public String getCurMysqlIp() {
        return currAddress.getHostString();
    }

    public String getCurMysqlUrl() {
        return currAddress.getHostString() + ":" + currAddress.getPort();
    }

    public int getCurMysqlPort() {
        return currAddress.getPort();
    }

    public String getMysqlUserName() {
        return mysqlUserName;
    }

    public String getMysqlPassWd() {
        return mysqlPassWd;
    }

    public String getServerName() {
        return serverName;
    }

    public String getJobName() {
        if (StringUtils.isEmpty(jobName)) {
            jobName = currAddress.getHostString() + ":" + currAddress.getPort() + ":" + serverName;
        }
        return jobName;
    }

    public String getBakJobName() {
        return getBakMysqlIp() + ":" + getBakMysqlPort() + ":" + getServerName();
    }

    private InetSocketAddress getAddress(String instName) {
        if (instName == null) {
            LOGGER.error("instName is null");
            return null;
        }
        if (!instName.contains(":")) {
            LOGGER.error("instName {} format error", instName);
            return null;
        }

        String[] ipPort = instName.split(":");
        return new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
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

    public int getMaxUnackedLogPositions() {
        return maxUnackedLogPositions;
    }

    public void setMaxUnackedLogPositions(int maxUnackedLogPositions) {
        this.maxUnackedLogPositions = maxUnackedLogPositions;
    }

    public String getAlarmPerson() {

        if (jobAlarmPerson.size() <= 0) {
            return null;
        }

        return StringUtils.join(jobAlarmPerson, ";");
    }

    public void setAlarmPerson(String person) {

        if (person == null || person.trim().length() <= 0) {
            return;
        }

        String[] persons = StringUtils.split(person, ";");
        for (String p : persons) {
            jobAlarmPerson.add(p.toLowerCase().trim());
        }
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

    public String getMstMysqlIp() {
        return dbAddrList.get(0).getHostString();
    }

    public int getMstMysqlPort() {
        return dbAddrList.get(0).getPort();
    }

    public String getBakMysqlIp() {
        int index = (dbIndex.get() + 1) % dbAddrList.size();
        return dbAddrList.get(index).getHostString();
    }

    public int getBakMysqlPort() {
        int index = (dbIndex.get() + 1) % dbAddrList.size();
        return dbAddrList.get(index).getPort();
    }

    public String getBakMysqlUrl() {
        int index = (dbIndex.get() + 1) % dbAddrList.size();
        InetSocketAddress bkAddr = dbAddrList.get(index);
        if (ObjectUtils.isNotEmpty(bkAddr) && StringUtils.isNotBlank(bkAddr.getHostString())) {
            return bkAddr.getHostString() + ":" + bkAddr.getPort();
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
        this.dbAddrList.add(new InetSocketAddress(host, port));
    }

    public LogPosition getStartPos() {
        return startPos;
    }

    public void resetDbInfo(String url, String bakUrl) {
        String mysqlIp = DBSyncUtils.getHost(url);
        int mysqlPort = DBSyncUtils.getPort(url);
        List<InetSocketAddress> newDbAddressList = Lists.newArrayList();
        newDbAddressList.add(new InetSocketAddress(mysqlIp, mysqlPort));

        // if bakUrl is valid, add it to newDbAddressList
        String bakMysqlIp = DBSyncUtils.getHost(bakUrl);
        int bakMysqlPort = DBSyncUtils.getPort(bakUrl);
        if (StringUtils.isBlank(bakMysqlIp) || bakMysqlPort <= 0) {
            LOGGER.warn("bakMysqlIp {} is empty or bakMysqlPort {} <=0, set to empty", bakMysqlIp, bakMysqlPort);
        } else {
            newDbAddressList.add(new InetSocketAddress(bakMysqlIp, bakMysqlPort));
        }
        dbAddrList = newDbAddressList;
    }

    public void doReset() throws IllegalStateException {
        if (status == TaskStat.SWITCHING) {
            throw new IllegalStateException("current job is switching");
        }

        dbIndex.set(0);
        ADDR_UPDATE.set(DBSyncJobConf.this, dbAddrList.get(dbIndex.get()));

        STATUS_UPDATER.set(DBSyncJobConf.this, TaskStat.NORMAL);
    }

    public void doSwitch() throws IllegalStateException {
        if (status == TaskStat.SWITCHING || status == TaskStat.RESETTING) {
            throw new IllegalStateException("current job is switching or resetting");
        }

        int index = dbIndex.incrementAndGet() % dbAddrList.size();
        ADDR_UPDATE.set(DBSyncJobConf.this, dbAddrList.get(index));
        STATUS_UPDATER.set(DBSyncJobConf.this, TaskStat.SWITCHING);
    }

    public boolean bSwitched() {
        return status == JobStat.TaskStat.SWITCHING
                || status == JobStat.TaskStat.SWITCHED;
    }

    @Override
    public String toString() {
        return "DBSyncJobConf{mysqlUserName='" + mysqlUserName + '\'' + ", mysqlPassWd='" + mysqlPassWd + '\''
                + ", charset=" + charset + ", dbAddrList=" + dbAddrList + ", startPos=" + startPos + '\'' + '}';
    }

    public List<InetSocketAddress> getDbAddrList() {
        return dbAddrList;
    }

    public void setDbAddrList(List<InetSocketAddress> dbAddrList) {
        this.dbAddrList = dbAddrList;
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
}
