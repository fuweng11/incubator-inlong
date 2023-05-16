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
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.entites.JobMetricInfo;
import org.apache.inlong.agent.metrics.dbsync.DbSyncMetricSink;
import org.apache.inlong.agent.metrics.dbsync.StatisticInfo;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.SnowFlake;
import org.apache.inlong.sdk.dataproxy.utils.ConcurrentHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_LOCAL_IP;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_REPORT_LATEST_POSITION_PERIOD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_REPORT_LATEST_POSITION_PERIOD_MSEC;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AGENT_UNIQ_ID;
import static org.apache.inlong.agent.metrics.dbsync.DbSyncMetricSink.DATA_KEY_DELIMITER;

public class DBSyncMetric extends Thread {

    private final Logger logger = LogManager.getLogger(this.getClass());
    protected final Logger jobSendMetricLogger = LogManager.getLogger("jobSendMetric");
    protected volatile boolean metricSenderRunning = true;
    protected LinkedBlockingQueue<StatisticInfo> sInfos;

    private LogPosition maxLogPosition = null;

    /**
     * key: serverId, value: snowflake
     */
    private ConcurrentHashMap<Long, SnowFlake> idGeneratorMap;

    private ConcurrentHashMap<String, AtomicLong> statisticData;
    private ConcurrentHashMap<String, AtomicLong> statisticDataPackage;

    private LogPosition newestLogPosition = null;
    private LogPosition oldestLogPosition = null;

    private LogPosition currentLogPosition;

    private DbSyncMetricSink dbSyncMetricSink;

    private String pulsarCluster = null;

    protected volatile boolean hasInited = false;

    private String serverName;

    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();

    private static volatile DBSyncMetric dbSyncMetricJob = null;

    private DBSyncJob dbSyncJob;

    private ScheduledExecutorService positionUpdateExecutor;

    private ConcurrentHashMap<String, ConcurrentHashSet<String>> positionConf = new ConcurrentHashMap<>();

    public void addDBSyncMetric(String inlongGroupID, String inlongStramID) {
        logger.info("addDBSyncMetric [{}] , {}", inlongGroupID, inlongStramID);
        positionConf.putIfAbsent(inlongGroupID, new ConcurrentHashSet<>());
        if (positionConf.get(inlongGroupID) != null) {
            positionConf.get(inlongGroupID).add(inlongStramID);
        }
    }

    public LogPosition getMaxLogPositionFromCache() {
        return newestLogPosition;
    }

    public LogPosition getOldestLogPositionFromCache() {
        return oldestLogPosition;
    }

    DBSyncMetricThread dbSyncMetricThread = new DBSyncMetricThread();

    public DBSyncMetric(DBSyncJob dbSyncJob) {
        this.statisticData = new ConcurrentHashMap<>();
        this.statisticDataPackage = new ConcurrentHashMap<>();
        this.sInfos = new LinkedBlockingQueue<>();
        this.idGeneratorMap = new ConcurrentHashMap<>();
        this.dbSyncJob = dbSyncJob;

        positionUpdateExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory(dbSyncJob.getDBSyncJobConf().getJobName() + "-max-position-updater"));
        positionUpdateExecutor.scheduleWithFixedDelay(maxPositionUpdateTask(),
                DBSYNC_REPORT_LATEST_POSITION_PERIOD_MSEC, agentConf.getLong(DBSYNC_REPORT_LATEST_POSITION_PERIOD,
                        DBSYNC_REPORT_LATEST_POSITION_PERIOD_MSEC),
                TimeUnit.MILLISECONDS);

    }

    public void init(String pulsarCluster, String serverName) {
        synchronized (DBSyncMetric.this) {
            if (hasInited) {
                return;
            }
            this.pulsarCluster = pulsarCluster;
            this.serverName = serverName;
            dbSyncMetricSink = new DbSyncMetricSink(pulsarCluster);
            hasInited = true;
        }
        logger.info("DBSyncMetric [{}] ,pulsarCluster {}", serverName, pulsarCluster);
    }

    public void start() {
        if (!hasInited) {
            logger.error("DBSyncMetric is not init! ");
            return;
        }
        logger.info("DBSyncMetric  start.");
        dbSyncMetricSink.start(this.serverName);
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

                        // LogPosition newPosition = jobNewestPosition.get(sInfo.getInstName());
                        LogPosition newPosition = dbSyncJob.getNewestLogPosition();
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
        logger.debug("sendStatMetric2 {}", sInfo);
        if (cnt != null && packageCnt != null) {
            sendMetricsToPulsar(sInfo, cnt, newestPosition,
                    oldestPosition, JobStat.State.RUN.name());
        }
    }

    public void sendMetricsToPulsar(StatisticInfo sInfo, long lineCnt,
            LogPosition newestPosition, LogPosition oldestPosition, String jobStat) {
        logger.debug("sendMetricsToPulsar2 {}", sInfo);
        Long newestPositionPos = 0L;
        String newestPositionBinlogName = "";
        String oldestPositionBinlogName = "";
        if (newestPosition == null || newestPosition.getPosition() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("serverId:{} bid:{} tid:{} instName:{} newestPosition is null!",
                        sInfo.getJobId(), sInfo.getGroupID(),
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
                        sInfo.getJobId(), sInfo.getGroupID(),
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
                        .jobId(sInfo.getJobId())
                        .serverName(serverName)
                        .jobStat(jobStat).build();
        if (lineCnt > 0) {
            jobSendMetricLogger.info(info);
        }

        if (dbSyncMetricSink != null) {
            dbSyncMetricSink.sendData(info);
            currentLogPosition = logPosition;
        }
    }

    public void addStatisticInfo(String key, String groupID, String streamID, long timeStample,
            long msgCnt, LogPosition latestLogPosition, String jobID) {
        AtomicLong statistic = statisticData.get(key);
        if (statistic == null) {
            statisticData.putIfAbsent(key, new AtomicLong(msgCnt));
            try {
                sInfos.put(new StatisticInfo(groupID, streamID, timeStample, latestLogPosition, jobID, key));
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

    private Runnable maxPositionUpdateTask() {
        return () -> {
            try {
                maxLogPosition = dbSyncJob.getMaxLogPosition();
                String dataKey = getDataKey(System.currentTimeMillis(), dbSyncJob.getDBSyncJobConf().getJobName());
                for (Map.Entry<String, ConcurrentHashSet<String>> entry : positionConf.entrySet()) {
                    sendPosition(entry.getKey(), entry.getValue(), dataKey);
                }
            } catch (Exception e) {
                logger.error("maxPositionUpdateTask errr {}", e.getMessage());
            }
        };
    }

    private String getDataKey(Long dateTime, String jobID) {
        return String.join(DATA_KEY_DELIMITER, String.valueOf(dateTime), jobID);
    }

    private void sendPosition(String inlongGroupID, ConcurrentHashSet<String> streamIDSet, String dataKey) {
        try {
            for (String streamID : streamIDSet) {
                StatisticInfo sInfo =
                        new StatisticInfo(inlongGroupID, streamID, System.currentTimeMillis(),
                                dbSyncJob.getSenderPosition(),
                                dbSyncJob.getDBSyncJobConf().getJobName(), dataKey);
                sendStatMetric(sInfo, 0L, 0L, maxLogPosition, null);
            }
        } catch (Exception e) {
            logger.error("sendPosition errr {}", e.getMessage());
        }
    }
}