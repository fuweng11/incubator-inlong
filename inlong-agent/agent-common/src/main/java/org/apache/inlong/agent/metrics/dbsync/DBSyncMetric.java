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

package org.apache.inlong.agent.metrics.dbsync;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.entites.JobMetricInfo;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.SnowFlake;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AGENT_UNIQ_ID;

public class DBSyncMetric extends Thread {

    private final Logger logger = LogManager.getLogger(this.getClass());
    protected final Logger jobSendMetricLogger = LogManager.getLogger("jobSendMetric");
    protected volatile boolean metricSenderRunning = true;
    protected LinkedBlockingQueue<StatisticInfo> sInfos;

    /**
     * key: serverId, value: snowflake
     */
    private ConcurrentHashMap<Long, SnowFlake> idGeneratorMap;

    private ConcurrentHashMap<String, AtomicLong> statisticData;
    private ConcurrentHashMap<String, AtomicLong> statisticDataPackage;

    private LogPosition newestLogPosition = null;
    private LogPosition oldestLogPosition = null;

    private DbSyncMetricSink dbSyncMetricSink;

    private String pulsarCluster = null;

    protected volatile boolean hasInited = false;

    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();

    private static volatile DBSyncMetric dbSyncMetricJob = null;

    public LogPosition getMaxLogPositionFromCache() {
        return newestLogPosition;
    }

    public LogPosition getOldestLogPositionFromCache() {
        return oldestLogPosition;
    }

    DBSyncMetricThread dbSyncMetricThread = new DBSyncMetricThread();

    public DBSyncMetric() {
        this.statisticData = new ConcurrentHashMap<>();
        this.statisticDataPackage = new ConcurrentHashMap<>();
        this.sInfos = new LinkedBlockingQueue<>();
        this.idGeneratorMap = new ConcurrentHashMap<>();
    }

    public void init(String pulsarCluster) {
        logger.info("DBSyncMetric pulsarCluster {}", pulsarCluster);
        synchronized (DBSyncMetric.this) {
            if (hasInited) {
                return;
            }
            this.pulsarCluster = pulsarCluster;
            dbSyncMetricSink = new DbSyncMetricSink(pulsarCluster);
            hasInited = true;
        }
    }

    public void start() {
        logger.info("DBSyncMetric  start.");
        dbSyncMetricSink.start("DBSyncMetricJob");
        dbSyncMetricThread.start();
    }

    class DBSyncMetricThread extends Thread {

        public void run() {
            logger.info("DBSyncMetricThread Thread start.");
            while (metricSenderRunning || !sInfos.isEmpty()) {
                try {
                    // deal statistic info
                    StatisticInfo sInfo;
                    HashMap<String, LogPosition> jobNewestPosition = new HashMap();
                    HashMap<String, LogPosition> jobStartPosition = new HashMap();
                    do {
                        // !!!should sort by datetime
                        sInfo = sInfos.peek();
                        if (sInfo == null) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("there's no sInfo, break");
                            }
                            break;
                        }
                        AtomicLong messageValue = new AtomicLong(0);
                        statisticData.compute(sInfo.getKey(), (k, v) -> {
                            if (v != null) {
                                messageValue.set(v.longValue());
                            }
                            sInfos.poll();
                            return null;
                        });

                        AtomicLong packageValue = new AtomicLong(0);
                        statisticDataPackage.compute(sInfo.getKey(), (k, v) -> {
                            if (v != null) {
                                packageValue.set(v.longValue());
                            }
                            return null;
                        });

                        LogPosition newPosition = jobNewestPosition.get(sInfo.getInstName());
                        LogPosition oldestPosition = jobStartPosition.get(sInfo.getInstName());
                        if (newPosition == null) {
                            newPosition = getMaxLogPositionFromCache();
                            if (newPosition != null) {
                                jobNewestPosition.put(sInfo.getInstName(), newPosition);
                                logger.info("Job instant name {} job newest Position: {}",
                                        sInfo.getInstName(),
                                        newPosition.getJsonObj());
                            }
                        }
                        if (oldestPosition == null) {
                            oldestPosition = getOldestLogPositionFromCache();
                            if (oldestPosition != null) {
                                jobStartPosition.put(sInfo.getInstName(), oldestPosition);
                                logger.info("Job instant name {} job oldest Position: {}",
                                        sInfo.getInstName(),
                                        oldestPosition.getJsonObj());
                            }
                        }
                        sendStatMetric(sInfo, messageValue.get(), packageValue.get(),
                                newPosition, oldestPosition);
                    } while (sInfo != null);
                    sleep(10 * 1000L);
                } catch (Throwable e) {
                    logger.error("getPositionUpdateTask has exception ", e);
                }
            }
        }
    }

    public void sendStatMetric(StatisticInfo sInfo, Long cnt, Long packageCnt,
            LogPosition newestPosition, LogPosition oldestPosition) {
        if (cnt != null && packageCnt != null) {
            sendMetricsToPulsar(sInfo, cnt, newestPosition,
                    oldestPosition, JobStat.State.RUN.name());
        }
    }

    public void sendMetricsToPulsar(StatisticInfo sInfo, long lineCnt,
            LogPosition newestPosition, LogPosition oldestPosition, String jobStat) {
        Long newestPositionPos = 0L;
        String newestPositionBinlogName = "";
        String oldestPositionBinlogName = "";
        if (newestPosition == null || newestPosition.getPosition() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("serverId:{} bid:{} tid:{} instName:{} newestPosition is null!",
                        sInfo.getServerId(), sInfo.getGroupID(),
                        sInfo.getStreamID(), sInfo.getInstName());
            }
        } else {
            newestPositionPos = newestPosition.getPosition().getPosition();
            newestPositionBinlogName = newestPosition.getPosition().getJournalName();
        }

        if (oldestPosition != null && oldestPosition.getPosition() != null) {
            oldestPositionBinlogName = oldestPosition.getPosition().getJournalName();
        }
        LogPosition logPosition = sInfo.getLatestLogPosition();
        Long currentLogPositionPos = 0L;
        Long currentLogPositionTimestamp = 0L;
        String currentLogPositionBinlogName = "";
        String currentPositionAddr = "";
        if (logPosition == null || logPosition.getPosition() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("serverId:{} bid:{} tid:{} instName:{} no current position",
                        sInfo.getServerId(), sInfo.getGroupID(),
                        sInfo.getStreamID(), sInfo.getInstName());
            }
        } else {
            currentLogPositionPos = logPosition.getPosition().getPosition();
            currentLogPositionTimestamp = logPosition.getPosition().getTimestamp();
            currentLogPositionBinlogName = logPosition.getPosition().getJournalName();
            if (logPosition.getIdentity() != null && logPosition.getIdentity().getSourceAddress() != null) {
                currentPositionAddr =
                        logPosition.getIdentity().getSourceAddress().getAddress().getHostAddress();
            }
        }

        JobMetricInfo info =
                JobMetricInfo.builder()
                        .groupID(sInfo.getGroupID()).dataTime(sInfo.getTimestamp())
                        .cnt(lineCnt).dbCurrentTime(currentLogPositionTimestamp)
                        .dbCurrentPosition(currentLogPositionPos)
                        .dbCurrentBinlog(currentLogPositionBinlogName)
                        .dbIp(currentPositionAddr)
                        .dbNewestPosition(newestPositionPos)
                        .dbNewestBinlog(newestPositionBinlogName)
                        .dbOldestBinlog(oldestPositionBinlogName)
                        .clusterId(agentConf.get(AGENT_CLUSTER_NAME, ""))
                        .idx(generateSnowId(1000))
                        .ip(agentConf.get(AGENT_LOCAL_IP, DEFAULT_AGENT_UNIQ_ID))
                        .reportTime(System.currentTimeMillis())
                        .streamID(sInfo.getStreamID())
                        .serverId(sInfo.getServerId())
                        .jobStat(jobStat).build();
        if (lineCnt > 0) {
            jobSendMetricLogger.info(info);
        }
        dbSyncMetricSink.sendData(info);
    }

    public void addStatisticInfo(String key, String groupID, String streamID, long timeStample,
            long msgCnt, LogPosition latestLogPosition, String serverId) {
        AtomicLong statistic = statisticData.get(key);
        if (statistic == null) {
            statisticData.putIfAbsent(key, new AtomicLong(msgCnt));
            try {
                sInfos.put(new StatisticInfo(groupID, streamID, timeStample, latestLogPosition, serverId, key));
            } catch (InterruptedException e) {
                logger.error("put {}#{} StatisticInfo {} error, ", groupID, streamID, timeStample,
                        e);
            }
        } else {
            statistic.addAndGet(msgCnt);
        }

        AtomicLong dataPackage = statisticDataPackage.get(key);
        if (dataPackage == null) {
            statisticDataPackage.putIfAbsent(key, new AtomicLong(msgCnt));
        } else {
            dataPackage.addAndGet(msgCnt);
        }
    }

    /**
     * generate snow id using serverId;
     *
     * @param serverId
     */
    private long generateSnowId(long serverId) {
        idGeneratorMap.computeIfAbsent(serverId, k -> new SnowFlake(serverId));
        return idGeneratorMap.get(serverId).nextId();
    }
}