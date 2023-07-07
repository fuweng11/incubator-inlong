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
import org.apache.inlong.agent.entites.JobMetricInfo;
import org.apache.inlong.agent.metrics.MetricReport;
import org.apache.inlong.agent.metrics.dbsync.DbSyncMetricSink;
import org.apache.inlong.agent.metrics.dbsync.StatisticInfo;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.SnowFlake;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.agent.constant.AgentConstants.*;

public class DbAgentMetricManager implements MetricReport {

    private final Logger logger = LogManager.getLogger(this.getClass());
    protected final Logger jobSendMetricLogger = LogManager.getLogger("jobSendMetric");
    protected volatile boolean metricSenderRunning = true;
    protected volatile boolean preStop = false;
    /**
     * key: serverId, value: snowflake
     */
    private ConcurrentHashMap<Long, SnowFlake> idGeneratorMap;
    private ConcurrentHashMap<String, StatisticInfo> statisticData;

    private DbSyncMetricSink dbSyncMetricSink;

    protected volatile boolean hasInited = false;

    private String dbJobId;

    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();

    private DBSyncJob dbSyncJob;

    DBSyncMetricThread dbSyncMetricThread = new DBSyncMetricThread();

    public DbAgentMetricManager(DBSyncJob dbSyncJob) {
        this.statisticData = new ConcurrentHashMap<>();
        this.idGeneratorMap = new ConcurrentHashMap<>();
        this.dbSyncJob = dbSyncJob;
    }

    public void init(String pulsarCluster, String dbJobId) {
        synchronized (DbAgentMetricManager.this) {
            if (hasInited) {
                return;
            }
            hasInited = true;
            this.dbJobId = dbJobId;
            dbSyncMetricSink = new DbSyncMetricSink(pulsarCluster);
        }
        logger.info("DBSyncMetric [{}] ,pulsarCluster {}", dbJobId, pulsarCluster);
    }

    public void start() {
        if (!hasInited) {
            logger.info("DBSyncMetric has been init! ");
            return;
        }
        logger.info("DBSyncMetric  start.");
        dbSyncMetricSink.start(this.dbJobId);
        dbSyncMetricThread.start();
    }

    public void stop() {
        preStop = true;
        if (dbSyncMetricSink != null) {
            while (statisticData != null && !statisticData.isEmpty()) {
                try {
                    logger.warn("Now wait in sInfos:{} to stop!", statisticData.size());
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }
            metricSenderRunning = false;
            dbSyncMetricSink.stop();
        }
    }

    class DBSyncMetricThread extends Thread {

        public void run() {
            logger.info("DBSyncMetricThread Thread start.");
            while (metricSenderRunning || !statisticData.isEmpty()) {
                try {
                    // deal statistic info
                    ConcurrentHashMap.KeySetView<String, StatisticInfo> keys = statisticData.keySet();
                    if (keys != null) {
                        keys.stream().forEach((key) -> {
                            statisticData.compute(key, (k, v) -> {
                                LogPosition newPosition =
                                        dbSyncJob.getReadJob().getJobPositionManager().getNewestLogPositionFromCache();
                                LogPosition oldestPosition =
                                        dbSyncJob.getReadJob().getJobPositionManager().getOldestLogPositionFromCache();
                                sendStatMetric(v, v.getCnt().get(), newPosition, oldestPosition);
                                return null;
                            });
                        });
                    }
                    /*
                     * 每分钟输出一下指标
                     */
                    for (int i = 0; i < 60; i++) {
                        if (preStop) {
                            break;
                        }
                        sleep(1 * 1000L);
                    }
                } catch (Throwable e) {
                    logger.error("getPositionUpdateTask has exception ", e);
                }
            }
        }
    }

    public void sendStatMetric(StatisticInfo sInfo, Long cnt,
            LogPosition newestPosition, LogPosition oldestPosition) {
        logger.debug("sendStatMetric2 {}", sInfo);
        if (cnt != null) {
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
                logger.debug("dbJobId:{} groupId:{} streamId:{} newestPosition is null!",
                        sInfo.getDbJobId(), sInfo.getGroupID(), sInfo.getStreamID());
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
                logger.debug("dbJobId:{} groupId:{} streamId:{} no current position",
                        sInfo.getDbJobId(), sInfo.getGroupID(), sInfo.getStreamID());
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
                        .idx(generateSnowId(dbSyncJob.getJobServerId()))
                        .ip(agentConf.get(AGENT_LOCAL_IP, DEFAULT_AGENT_UNIQ_ID))
                        .reportTime(System.currentTimeMillis())
                        .streamID(sInfo.getStreamID())
                        .jobId(sInfo.getDbJobId())
                        .serverName(dbJobId)
                        .jobStat(jobStat).build();
        if (lineCnt > 0) {
            jobSendMetricLogger.info(info);
        }

        if (dbSyncMetricSink != null) {
            dbSyncMetricSink.sendData(info);
        }
    }

    public void addStatisticInfo(String groupID, String streamID, long timeStamp,
            long msgCnt, LogPosition latestLogPosition, String jobID, String key) {
        statisticData.compute(key, (k, v) -> {
            if (v == null) {
                StatisticInfo info = new StatisticInfo(groupID, streamID, timeStamp, latestLogPosition, jobID, key);
                info.addCnt(msgCnt);
                v = info;
            } else {
                v.updateTimestamp(timeStamp);
                v.updateLatestLogPosition(latestLogPosition);
                v.addCnt(msgCnt);
            }
            return v;
        });
    }

    /**
     * generate snow id using serverId;
     *
     * @param serverId
     */
    private long generateSnowId(long serverId) {
        idGeneratorMap.computeIfAbsent(serverId, k -> new SnowFlake(serverId, true));
        return idGeneratorMap.get(serverId).nextId();
    }

    @Override
    public String report() {
        dbSyncMetricSink.report();
        return "statisticData:" + statisticData.size();
    }
}