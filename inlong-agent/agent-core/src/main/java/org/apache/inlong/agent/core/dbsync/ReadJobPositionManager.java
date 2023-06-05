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

package org.apache.inlong.agent.core.dbsync;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.core.dbsync.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.metrics.MetricReport;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.protocol.position.EntryPosition;
import org.apache.inlong.agent.mysql.protocol.position.LogIdentity;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_ACK_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MSG_INDEX_KEY;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_ACK_THREAD_NUM;

/**
 * Each DBSyncReadOperator using a PositionControl to record and manage its positions info
 */
public class ReadJobPositionManager implements MetricReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadJobPositionManager.class);

    private String jobName;

    private DBSyncJobConf jobConf;

    private final List<AckLogPositionThread> handleAckLogPositionThreadList = new LinkedList<>();

    protected LinkedBlockingQueue<LogPosition> ackLogPositionList = new LinkedBlockingQueue<>();

    private volatile LogPosition sendAndAckedLogPosition = null;

    // flow control: maximum unacked message
    private final Semaphore semaphore;

    protected volatile boolean running = false;
    // store successful sink-sent positions

    protected AtomicLong waitAckCnt;

    private Long lastPrintTimeStamp;
    private long printStatIntervalMs = 60 * 1000 * 2;

    private final ConcurrentSkipListSet<LogPosition> sendLogPositionCache = new ConcurrentSkipListSet<>();
    // record eventLog positions while parseThread parsing eventLog

    private volatile MysqlConnection metaCon = null;

    private volatile boolean isNeedUpdateLogPosition = false;

    private ScheduledExecutorService positionUpdateExecutor;

    private ScheduledExecutorService parseLogPositionMonitorExecutor;

    private volatile LogPosition newestLogPosition = null;

    private volatile LogPosition oldestLogPosition = null;

    public ReadJobPositionManager(DBSyncJobConf jobConf) {
        this.jobConf = jobConf;
        this.jobName = jobConf.getDbJobId();
        this.waitAckCnt = new AtomicLong(0);
        int maxUnackedLogPositionsUnackedMessages = jobConf.getMaxUnAckedLogPositions();
        semaphore = new Semaphore(maxUnackedLogPositionsUnackedMessages);
        lastPrintTimeStamp = Instant.now().toEpochMilli();

        positionUpdateExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory(jobName + "-log-position-updater"));
        positionUpdateExecutor.scheduleWithFixedDelay(getLogPositionUpdateTask(),
                30 * 1000, 30 * 1000, TimeUnit.MILLISECONDS);

        parseLogPositionMonitorExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory(jobName
                        + "-parse-log-position-monitor"));
        parseLogPositionMonitorExecutor.scheduleWithFixedDelay(
                getJobSendAndAckedPositionMonitorTask(),
                1000, 30 * 1000, TimeUnit.MILLISECONDS);

    }

    public DBSyncJobConf getJobConf() {
        return jobConf;
    }

    public LogPosition getSendAndAckedLogPosition() {
        return sendAndAckedLogPosition;
    }

    /**
     * if receiving ack response, update sender pos and flush it to JobHa
     */
    public void updateSendAndAckedPosition(LogPosition ackedPos, long pkgIndexId) {
        // flush JobHa pos
        JSONObject jsonObject = ackedPos.getJsonObj();
        jsonObject.put(DBSYNC_MSG_INDEX_KEY, pkgIndexId);
        JobHaDispatcherImpl.getInstance().updatePosition(jobName, jsonObject.toJSONString());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("updateSendAndAckedPosition jobName = {} pkgIndexId = {} position = {} ", jobName, pkgIndexId,
                    ackedPos);
        }

    }

    private String getSyncIdFromInstanceName(String key) {
        if (key != null) {
            String[] splitA = key.split(":");
            if (splitA.length >= 3) {
                return splitA[2];
            }
        }
        return null;
    }

    public void ackSendPosition(BatchProxyMessage sendData) {
        if (sendData == null || CollectionUtils.isEmpty(sendData.getPositions())) {
            LOGGER.warn("ackSendPosition position is empty jobName {}, groupId {}, streamId",
                    jobName, sendData.getGroupId(), sendData.getStreamId());
            return;
        }
        // add ack log pos to handle-list
        for (Pair<LogPosition, Long> pair : sendData.getPositions()) {
            if (pair.getLeft() == null) {
                LOGGER.warn("{} ackSendPosition is null", jobName);
                return;
            }
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("ackSendPosition position = {}/{}", jobName, pair.getLeft());
                }
                ackLogPositionList.put(pair.getLeft());
            } catch (InterruptedException e) {
                LOGGER.error("ackSendPosition has exception e", e);
            }
        }

        // decrease wait ack num
        if (CollectionUtils.isNotEmpty(sendData.getDataList())) {
            int msgCnt = -sendData.getDataList().size();
            updateWaitAckCnt(msgCnt);
        }
    }

    public void ackSendPosition(LogPosition logPosition) {
        if (logPosition != null) {
            try {
                ackLogPositionList.put(logPosition);
            } catch (InterruptedException e) {
                LOGGER.error("ackLogPositionList has exception e = {}", e);
            }
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ackSendPosition is null");
            }
        }
    }

    public void updateWaitAckCnt(int cnt) {
        long ackResult = waitAckCnt.addAndGet(cnt);
        if (ackResult < 0) {
            LOGGER.debug("ackCnt is: {}, ackResult is: {}", cnt, ackResult);
        }
    }

    /**
     * Before ParseThread sending data, add the data position to sendLogPositionCache.
     */
    public void addLogPositionToSendCache(LogPosition logPosition) {
        try {
            long currentTimeStamp = Instant.now().toEpochMilli();
            semaphore.acquire();
            boolean result = sendLogPositionCache.add(logPosition);
            if (LOGGER.isDebugEnabled() && ((currentTimeStamp - lastPrintTimeStamp) > printStatIntervalMs)) {
                LOGGER.debug(
                        "[{}], logPosition {}, cacheLogPosition position in cache [{}], cache availablePermits = {}",
                        jobName, logPosition, sendLogPositionCache.size(), semaphore.availablePermits());
                lastPrintTimeStamp = currentTimeStamp;
            }
            if (!result) {
                LOGGER.warn(
                        "[{}], logPosition {}, cacheLogPosition position in cache [{}], cache availablePermits = {}",
                        jobName, logPosition, sendLogPositionCache.size(), semaphore.availablePermits());
            }
        } catch (Exception e) {
            LOGGER.error("cacheLogPosition has exception e = ", e);
        }
    }

    /**
     * Create ackLogPosition threadLists and start them.
     */
    public void start() {
        running = true;
        int ackThreadNum = AgentConfiguration.getAgentConf().getInt(DBSYNC_ACK_THREAD_NUM, DEFAULT_ACK_THREAD_NUM);
        for (int i = 0; i < ackThreadNum; i++) {
            AckLogPositionThread thread = new AckLogPositionThread();
            thread.setName("AckLogPosition-" + i + "-" + this.jobName);
            thread.start();
            handleAckLogPositionThreadList.add(thread);
        }
    }

    /**
     * Stop ackLogPosition threadLists.
     */
    public void stop() {
        running = false;
        for (Thread thread : handleAckLogPositionThreadList) {
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    /**
     * Clear all sendLogPosition and eventLogPosition.
     */
    public void clearCache() {
        int needReleasePermit = sendLogPositionCache.size();
        sendLogPositionCache.clear();
        semaphore.release(needReleasePermit);
    }

    public LogPosition getNewestLogPosition(MysqlConnection con) {
        LogPosition logPosition = null;
        if (con != null) {
            MysqlConnection metaCon = con;
            try {
                EntryPosition maxEntryPos = findEndPosition(metaCon, false);
                if (maxEntryPos == null) {
                    return null;
                }
                logPosition = new LogPosition();
                logPosition.setPosition(maxEntryPos);

                LogIdentity identity = new LogIdentity(metaCon.getConnector().getAddress(), -1L);
                logPosition.setIdentity(identity);
                newestLogPosition = logPosition;
            } catch (Throwable t) {
                LOGGER.warn("can't get max position " + ExceptionUtils.getStackTrace(t));
            } finally {
                LOGGER.info("GetMaxLogPosition finished! MaxLogPosition {}", (logPosition == null ? ""
                        : logPosition.getPosition()));
            }
        }
        return logPosition;
    }

    public EntryPosition findEndPosition(MysqlConnection mysqlConnection, boolean needRetryConnection) {
        try {
            EntryPosition endPosition;
            synchronized (mysqlConnection) {
                if (!mysqlConnection.isConnected()) {
                    if (needRetryConnection) {
                        mysqlConnection.connect();
                    } else {
                        return null;
                    }
                }
                ResultSetPacket packet = mysqlConnection.query("show master status");
                List<String> fields = packet.getFieldValues();
                if (DBSyncUtils.isCollectionsEmpty(fields)) {
                    throw new CanalParseException(
                            "command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,"
                                    + "REPLICATION CLIENT privilege(s) for this operation");
                }
                endPosition = new EntryPosition(fields.get(0), Long.parseLong(fields.get(1)));
            }
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException("command : 'show master status' has an error!"
                    + mysqlConnection.getConnector().getAddress().toString(), e);
        }
    }

    public LogPosition getOldestLogPosition(MysqlConnection con) {
        LogPosition logPosition = null;
        if (con != null) {
            MysqlConnection metaCon = con;
            try {
                EntryPosition maxEntryPos = findStartPositionCmd(metaCon);
                logPosition = new LogPosition();
                logPosition.setPosition(maxEntryPos);

                LogIdentity identity = new LogIdentity(metaCon.getConnector().getAddress(), -1L);
                logPosition.setIdentity(identity);
                oldestLogPosition = logPosition;
            } catch (Throwable t) {
                LOGGER.warn("can't get start position " + ExceptionUtils.getStackTrace(t));
            } finally {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("GetStartLogPosition finished!");
                }
            }
        }
        return logPosition;
    }

    public LogPosition getNewestLogPositionFromCache() {
        return newestLogPosition;
    }

    public LogPosition getOldestLogPositionFromCache() {
        return oldestLogPosition;
    }

    protected EntryPosition findStartPositionCmd(MysqlConnection mysqlConnection) {
        try {
            synchronized (mysqlConnection) {
                ResultSetPacket packet = mysqlConnection.query("show binlog events limit 1");
                List<String> fields = packet.getFieldValues();
                if (DBSyncUtils.isCollectionsEmpty(fields)) {
                    throw new CanalParseException(
                            "command : 'show binlog events limit 1' has an error! pls check."
                                    + " you need (at least one of) the SUPER,REPLICATION CLIENT "
                                    + "privilege(s) for this operation");
                }
                return new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            }
        } catch (IOException e) {
            throw new CanalParseException("command : 'show binlog events limit 1' has an error!", e);
        }

    }

    private long comparePosition(LogPosition oldPosition, LogPosition newPosition) {
        long cmpPosition = 0;
        if (oldPosition != null && newPosition != null) {
            EntryPosition oldEntryPosition = oldPosition.getPosition();
            EntryPosition newEntryPosition = newPosition.getPosition();
            if (oldEntryPosition != null && newEntryPosition != null) {
                Long oldTime = oldEntryPosition.getTimestamp();
                Long newTime = newEntryPosition.getTimestamp();
                if (oldTime != null && newTime != null) {
                    cmpPosition = (newTime - oldTime);
                }
                if (cmpPosition != 0L) {
                    return cmpPosition;
                }
                String oldBinlogName = oldEntryPosition.getJournalName();
                String newBinlogName = newEntryPosition.getJournalName();
                if (StringUtils.isNotEmpty(oldBinlogName) && StringUtils.isNotEmpty(newBinlogName)) {
                    cmpPosition = newBinlogName.compareTo(oldBinlogName);
                }
                if (cmpPosition != 0L) {
                    return cmpPosition;
                }
                Long oldPos = oldEntryPosition.getPosition();
                Long newPos = newEntryPosition.getPosition();
                if (oldPos != null && newPos != null) {
                    cmpPosition = newPos.compareTo(oldPos);
                }
            }
        }
        return cmpPosition;
    }

    public void updateMetaConnection(MysqlConnection metaCon) {
        this.metaCon = metaCon;
    }

    private Runnable getJobSendAndAckedPositionMonitorTask() {
        return () -> {
            try {
                handleAckLogPositionThreadList.stream().forEach((ackE) -> {
                    if (sendAndAckedLogPosition == null) {
                        sendAndAckedLogPosition = ackE.getFirstAckLogPosition();
                    } else {
                        LogPosition ackELogPosition = ackE.getFirstAckLogPosition();
                        if (ackELogPosition != null) {
                            long cmp = comparePosition(sendAndAckedLogPosition,
                                    ackELogPosition);
                            if (cmp > 0) {
                                sendAndAckedLogPosition = ackELogPosition;
                            }
                        }
                    }
                });
            } catch (Exception e) {
                LOGGER.error("getJobParsePositionMonitorTask has exception e = ", e);
            }
        };
    }

    private class AckLogPositionThread extends Thread {

        private volatile LogPosition firstAckLogPosition;

        public LogPosition getFirstAckLogPosition() {
            return firstAckLogPosition;
        }

        @Override
        public void run() {
            long lastTimeStamp = Instant.now().toEpochMilli();
            LOGGER.info("AckLogPositionThread started!");
            while (running) {
                try {
                    boolean hasRemoved = false;
                    LogPosition logPosition = ackLogPositionList.poll(1, TimeUnit.SECONDS);
                    if (logPosition != null) {
                        LogPosition tmp = sendLogPositionCache.first();
                        hasRemoved = sendLogPositionCache.remove(logPosition);
                        if (hasRemoved) {
                            if (tmp != null) {
                                firstAckLogPosition = tmp;
                            }
                            semaphore.release();
                        } else {
                            LOGGER.warn("[{}] ack log position is not exist in cache may has "
                                    + "error! Position = {}", jobName, logPosition);
                        }
                    }
                    long currentTimeStamp = Instant.now().toEpochMilli();
                    if (LOGGER.isDebugEnabled() && (currentTimeStamp - lastTimeStamp) > printStatIntervalMs) {
                        LOGGER.debug(
                                "[{}], AckLogPositionThread position in cache [{}], logPosition {}, cache size = {}",
                                jobName, hasRemoved, logPosition, sendLogPositionCache.size());
                        lastTimeStamp = currentTimeStamp;
                    }
                } catch (Throwable e) {
                    if (running) {
                        LOGGER.error("AckLogPositionThread has exception ", e);
                    }
                }
            }
        }
    }
    private Runnable getLogPositionUpdateTask() {
        return () -> {
            if (isNeedUpdateLogPosition) {
                MysqlConnection con = metaCon;
                LogPosition tmpNewestLogPosition = getNewestLogPosition(con);
                LogPosition tmpOldestLogPosition = getOldestLogPosition(con);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("UpdatePosition newestLogPosition ={} , oldestLogPosition = "
                            + "{}", tmpNewestLogPosition, tmpOldestLogPosition);
                }
            }
        };
    }

    public void setNeedUpdateLogPosition(boolean isNeedUpdateLogPosition) {
        this.isNeedUpdateLogPosition = isNeedUpdateLogPosition;
    }

    @Override
    public String report() {
        StringBuilder bs = new StringBuilder();
        if (sendAndAckedLogPosition != null) {
            bs.append("Current-ackPosition:[").append(sendAndAckedLogPosition.toMinString()).append("]");
        }
        if (newestLogPosition != null) {
            bs.append("|DB-newestPosition:[").append(newestLogPosition.toMinString()).append("]");
        }
        if (oldestLogPosition != null) {
            bs.append("|DB-oldestPosition:[").append(oldestLogPosition.toMinString()).append("]");
        }
        bs.append("|sendLogPositionCache:[").append(sendLogPositionCache.size()).append("]");
        bs.append("|ackLogPositionList:[").append(ackLogPositionList.size()).append("]");
        return bs.toString();
    }

}
