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

package org.apache.inlong.agent.core.monitor;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.core.job.JobManager;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.utils.HandleFailedMessage;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.sdk.dataproxy.network.HttpProxySender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MSG_INDEX_KEY;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_UPDATE_POSITION_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_UPDATE_POSITION_INTERVAL;

public class JobMonitor extends AbstractDaemon {

    protected static final Logger LOGGER = LoggerFactory.getLogger(JobMonitor.class);
    private static HttpProxySender proxySender;
    private final int updateZkInterval;
    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    private volatile boolean running = false;
    private ConcurrentHashMap<String, Long> sendedAlartMap;
    private HandleFailedMessage handleFailedMessage;
    private Random random = new Random();
    private boolean bStoped = false;
    private boolean monitorFlag = true;
    private long jobBlockTime = 0L;
    private JobManager jobManager;
    private JobHaDispatcherImpl jobHaDispatcher;

    //TODO:setUncaughtExceptionHandler
    public JobMonitor(AgentManager agentManager) {
        this.jobManager = agentManager.getJobManager();
        handleFailedMessage = HandleFailedMessage.getInstance();
        updateZkInterval = agentConf.getInt(DBSYNC_UPDATE_POSITION_INTERVAL, DEFAULT_UPDATE_POSITION_INTERVAL);
    }

    public static HttpProxySender getProxySender() {
        return proxySender;
    }

    @Override
    public void start() {
        submitWorker(getPositionUpdateTask());
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    private Runnable getPositionUpdateTask() {
        return () -> {
            while (isRunnable()) {
                try {
                    /*
                     * When there is no table name or field name matching rule,
                     *  it needs to be updated with the position parsed by binlog
                     */
                    for (Map.Entry<String, DBSyncJob> runningJob : jobManager.getRunningJobs().entrySet()) {
                        if (!runningJob.getValue().isRunning()) {
                            LOGGER.warn("runningJob [{}] is not running!", runningJob.getKey());
                            continue;
                        }
                        LogPosition storePos = getStorePosition(runningJob.getKey(),
                                runningJob.getValue());
                        if (storePos == null) {
                            LOGGER.warn("runningJob [{}] store position is null", runningJob.getKey());
                            continue;
                        }
                        LogPosition lastStorePosition =
                                jobManager.getRunningJobsLastStorePositionMap().get(runningJob.getKey());
                        if (lastStorePosition != null && lastStorePosition.equals(storePos)) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("position has stored, jobName = {} ,position = {}",
                                        runningJob.getKey(), lastStorePosition);
                            }
                            continue;
                        }
                        updatePosition(runningJob.getKey(), storePos,
                                runningJob.getValue().getPkgIndexId());
                        jobManager.getRunningJobsLastStorePositionMap().put(runningJob.getKey(), storePos);
                    }
                    TimeUnit.SECONDS.sleep(updateZkInterval);
                } catch (Throwable e) {
                    LOGGER.error("getPositionUpdateTask has exception ", e);
                }

            }
        };
    }

    public LogPosition getStorePosition(String jobName, DBSyncJob job) {
        if (job == null) {
            return null;
        }
        LogPosition storePos = null;
        if (StringUtils.isEmpty(jobName)) {
            if (job.getJobConf() != null) {
                jobName = job.getDBSyncJobConf().getJobName();
            }
        }
        //TODO: if storepos is null
//        if (storePos == null) {
//            storePos = job.getMinCacheSendLogPosition();
//            if (storePos == null) {
//                storePos = job.getMinCacheEventLogPosition();
//                if (LOGGER.isDebugEnabled()) {
//                    LOGGER.debug("[{}] getLogPosition = {}", jobName, storePos);
//                }
//                LogPosition minPos = job.getMinCacheSendLogPosition();
//                if (minPos != null) {
//                    storePos = minPos;
//                }
//            }
//        }
        return storePos;
    }

    private void updatePosition(String jobName, LogPosition ackedPos, long pkgIndexId) {
        JSONObject jsonObject = ackedPos.getJsonObj();
        jsonObject.put(DBSYNC_MSG_INDEX_KEY, pkgIndexId);
//        updateJobSenderPosition(jobName, ackedPos);
        jobHaDispatcher.updatePosition(getSyncIdFromInstanceName(jobName),
                jsonObject.toJSONString());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("updatePosition jobName = {} pkgIndexId = {} position = {} ", jobName,
                    pkgIndexId, ackedPos);
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

//    TODO:
//    public void addRecodeLogPosition(PackageData pkgData) {
//        jobManager.addRecodeLogPosition(pkgData);
//    }

    /**
     * generate uuid for the metric to be unique
     *
     * @return
     */
    private String getUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public void resetMonitorFlag() {
        monitorFlag = false;
    }

    public void stopMonitor() {
        running = false;
        while (!bStoped) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("", e);
            }
        }
    }
}
