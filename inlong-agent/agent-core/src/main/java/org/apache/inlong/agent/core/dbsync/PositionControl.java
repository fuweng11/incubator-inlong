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

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat.State;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_ACK_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MSG_INDEX_KEY;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_ACK_THREAD_NUM;

/**
 * Each DBSyncReadOperator using a PositionControl to record and manage its positions info
 */
public class PositionControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionControl.class);
    private final List<Thread> handleAckLogPositionThreadList = new LinkedList<>();
    // flow control: maximum unacked message
    private final Semaphore semaphore;
    private final DBSyncReadOperator readOperator;
    public volatile long pkgIndexId = 0L; // dbsync reader dump pkgIndexId
    protected volatile boolean running = false;
    // store successful sink-sent positions
    protected LinkedBlockingQueue<LogPosition> ackLogPositionList = new LinkedBlockingQueue<>();
    protected AtomicLong waitAckCnt;
    private String jobName;
    private DBSyncJobConf jobConf;
    private Long lastPrintTimeStamp;
    private long printStatIntervalMs = 60 * 1000 * 2;
    private LogPosition minEventLogPosition;
    private LogPosition senderPosition = null;
    private LogPosition lastStorePosition = null;
    // record sending positions, and remove position after ack (from ackLogPositionList)
    private final ConcurrentSkipListSet<LogPosition> sendLogPositionCache = new ConcurrentSkipListSet<>();
    // record eventLog positions while parseThread parsing eventLog
    private final ConcurrentSkipListSet<LogPosition> eventLogPositionCache = new ConcurrentSkipListSet<>();

    public PositionControl(DBSyncJobConf jobConf, DBSyncReadOperator readOperator) {
        this.jobConf = jobConf;
        this.jobName = jobConf.getJobName();
        this.readOperator = readOperator;
        this.waitAckCnt = new AtomicLong(0);
        int maxUnackedLogPositionsUnackedMessages = jobConf.getMaxUnackedLogPositions();
        semaphore = new Semaphore(maxUnackedLogPositionsUnackedMessages);
        lastPrintTimeStamp = Instant.now().toEpochMilli();

    }

    public DBSyncJobConf getJobConf() {
        return jobConf;
    }

    public LogPosition getSenderPosition() {
        return senderPosition;
    }

    public LogPosition getLastStorePosition() {
        return lastStorePosition;
    }

    public LogPosition getStorePositionFirstFromCache() {
        if (lastStorePosition == null) {
            LOGGER.debug("job {} lastStorePos is null, then init it", jobName);
            lastStorePosition = getStorePosition();
        }
        return lastStorePosition;
    }

    public LogPosition getStorePosition() {
        LOGGER.debug("job {} lastStorePos is {}, then init it", jobName, lastStorePosition);
        LogPosition storePos = getMinCacheSendLogPosition();
        if (storePos == null) {
            storePos = getMinCacheEventLogPosition();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] getLogPosition = {}", jobName, storePos);
            }
            LogPosition minPos = getMinCacheSendLogPosition();
            if (minPos != null) {
                storePos = minPos;
            }
        }
        return storePos;
    }

    private LogPosition getMinCacheSendLogPosition() {
        if (readOperator.getState() != State.RUN) {
            return null;
        }
        if (sendLogPositionCache.size() > 0) {
            LogPosition logPosition = null;
            try {
                logPosition = sendLogPositionCache.first();
            } catch (Exception e) {
                LOGGER.error("getMinCacheLogPosition has exception", e);
            }
            if (logPosition != null) {
                return new LogPosition(logPosition);
            }
        }
        return null;
    }

    private LogPosition getMinCacheEventLogPosition() {
        if (readOperator.getState() != State.RUN) {
            return null;
        }
        // if eventLogPositionCache is empty, use max processed position
        minEventLogPosition = readOperator.getMaxProcessedPosition();
        try {
            minEventLogPosition = eventLogPositionCache.first();
        } catch (Exception e) {
            if (!(e instanceof NoSuchElementException)) {
                LOGGER.warn("getMinCacheEventLogPosition has exception e", e);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("getMinCacheEventLogPosition has exception e", e);
                }
            }
        }
        return minEventLogPosition;
    }

    public void setMinEventLogPosition(LogPosition position) {
        minEventLogPosition = position;
    }

    /**
     * if receiving ack response, update sender pos and flush it to JobHa
     */
    public void updateSenderPosition(LogPosition ackedPos) {
        senderPosition = ackedPos;
        lastStorePosition = ackedPos;

        // flush JobHa pos
        JSONObject jsonObject = ackedPos.getJsonObj();
        jsonObject.put(DBSYNC_MSG_INDEX_KEY, pkgIndexId);
        JobHaDispatcherImpl.getInstance().updatePosition(getSyncIdFromInstanceName(jobName), jsonObject.toJSONString());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("updatePosition jobName = {} pkgIndexId = {} position = {} ", jobName, pkgIndexId, ackedPos);
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

    public void updateAckInfo(BatchProxyMessage sentData) {
        if (sentData == null || CollectionUtils.isEmpty(sentData.getPositions())) {
            return;
        }

        // add ack log pos to handle-list
        for (Pair<LogPosition, Long> pair : sentData.getPositions()) {
            if (pair.getLeft() == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{} ackSendPosition is null", jobName);
                }
                return;
            }
            try {
                ackLogPositionList.put(pair.getLeft());
            } catch (InterruptedException e) {
                LOGGER.error("ackLogPositionList has exception e", e);
            }
        }

        // decrease wait ack num
        if (CollectionUtils.isNotEmpty(sentData.getDataList())) {
            int msgCnt = -sentData.getDataList().size();
            updateWaitAckCnt(msgCnt);
        }
    }

    public void updateWaitAckCnt(int cnt) {
        long ackResult = waitAckCnt.addAndGet(cnt);
        if (ackResult < 0) {
            LOGGER.debug("ackCnt is: {}, ackResult is: {}", cnt, ackResult);
        }
    }

    public void addEventLogPosition(LogPosition eventLogPosition) {
        eventLogPositionCache.add(eventLogPosition);
    }

    public void removeEventLogPosition(LogPosition eventLogPosition) {
        eventLogPositionCache.remove(eventLogPosition);
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

    public boolean isReaderRunning() {
        return readOperator != null && readOperator.getState() == State.RUN;
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
        lastStorePosition = null;
    }

    /**
     * Clear all sendLogPosition and eventLogPosition.
     */
    public void clearCache() {
        int needReleasePermit = sendLogPositionCache.size();
        sendLogPositionCache.clear();
        eventLogPositionCache.clear();
        semaphore.release(needReleasePermit);
    }

    private class AckLogPositionThread extends Thread {

        @Override
        public void run() {
            long lastTimeStamp = Instant.now().toEpochMilli();
            LOGGER.info("AckLogPositionThread started!");
            while (running) {
                try {
                    boolean hasRemoved = false;
                    LogPosition logPosition = ackLogPositionList.poll(1, TimeUnit.SECONDS);
                    if (logPosition != null) {
                        hasRemoved = sendLogPositionCache.remove(logPosition);
                        if (hasRemoved) {
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

}
