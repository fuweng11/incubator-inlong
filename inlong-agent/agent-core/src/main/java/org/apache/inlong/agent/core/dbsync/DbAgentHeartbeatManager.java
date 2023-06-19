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

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.core.DbAgentManager;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.common.heartbeat.AbstractHeartbeatManager;
import org.apache.inlong.common.heartbeat.GroupHeartbeat;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.common.heartbeat.StreamHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeatRequest;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.inlong.agent.constant.AgentConstants.*;
import static org.apache.inlong.agent.constant.FetcherConstants.*;

/**
 * report heartbeat to inlong-manager
 */
public class DbAgentHeartbeatManager extends AbstractDaemon implements AbstractHeartbeatManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbAgentHeartbeatManager.class);
    private static DbAgentHeartbeatManager heartbeatManager = null;
    private final DbAgentJobManager jobManager;
    private final AgentConfiguration conf;
    private final HttpManager httpManager;
    private final String baseManagerUrl;
    private final String reportHeartbeatUrl;
    private final String dbSyncReportUrl;
    private final long connInterval;
    private final Random random = new Random();
    private final LinkedBlockingQueue<DbSyncHeartbeat> holdStopJobHeartbeats;
    private final String agentIp;
    private final int agentPort;
    private final String clusterName;
    private final String clusterTag;
    private final String inCharges;
    private final String nodeGroup;

    /**
     * Init heartbeat manager.
     */
    private DbAgentHeartbeatManager(DbAgentManager agentManager) {
        this.holdStopJobHeartbeats = new LinkedBlockingQueue<>();
        this.conf = AgentConfiguration.getAgentConf();
        this.connInterval = conf.getLong(DBSYNC_HEART_INTERVAL, DEFAULT_DBSYNC_HEART_INTERVAL);
        this.jobManager = agentManager.getJobManager();
        this.httpManager = new HttpManager(conf);
        this.baseManagerUrl = HttpManager.buildBaseUrl();
        this.reportHeartbeatUrl = buildReportHeartbeatUrl(baseManagerUrl);
        this.dbSyncReportUrl = buildDBSyncHbUrl();
        this.agentIp = AgentUtils.fetchLocalIp();
        this.agentPort = conf.getInt(AGENT_HTTP_PORT, DEFAULT_AGENT_HTTP_PORT);
        this.clusterName = conf.get(AGENT_CLUSTER_NAME);
        this.clusterTag = conf.get(AGENT_CLUSTER_TAG);
        this.inCharges = conf.get(AGENT_CLUSTER_IN_CHARGES);
        this.nodeGroup = conf.get(AGENT_NODE_GROUP);
    }

    public static DbAgentHeartbeatManager getInstance(DbAgentManager agentManager) {
        if (heartbeatManager == null) {
            synchronized (DbAgentHeartbeatManager.class) {
                if (heartbeatManager == null) {
                    heartbeatManager = new DbAgentHeartbeatManager(agentManager);
                }
            }
        }
        return heartbeatManager;
    }

    public static DbAgentHeartbeatManager getInstance() {
        if (heartbeatManager == null) {
            throw new RuntimeException("HeartbeatManager has not been initialized by agentManager");
        }
        return heartbeatManager;
    }

    public void putStopHeartbeat(DbSyncHeartbeat stopHb) {
        if (stopHb != null) {
            holdStopJobHeartbeats.offer(stopHb);
            LOGGER.warn("Stop Hb for dbJobId : {}", stopHb.getServerName());
        }
    }

    @Override
    public void start() throws Exception {
        // TODO:improve, merge two heartbeat report, need manager support
        // Now, agent automatic registration depends on the heartbeat module, must report
        submitWorker(heartbeatReportThread());
        submitWorker(dbSyncHbReportThread());
    }

    private Runnable dbSyncHbReportThread() {
        return () -> {
            LOGGER.info("DbSync-heartbeat-report thread start");
            while (isRunnable()) {
                try {
                    reportDBSyncHeartbeat();
                } catch (Throwable e) {
                    LOGGER.error("dbsync-heartbeat-report interrupted while report heartbeat", e);
                } finally {
                    AgentUtils.silenceSleepInMs(getDBSyncInterval());
                }
            }
        };
    }

    private Runnable heartbeatReportThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    HeartbeatMsg heartbeatMsg = buildHeartbeatMsg();
                    reportHeartbeat(heartbeatMsg);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(" {} report heartbeat to manager", heartbeatMsg);
                    }
                    SECONDS.sleep(heartbeatInterval());
                } catch (Throwable e) {
                    LOGGER.error("interrupted while report heartbeat", e);
                    // ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                }
            }
        };
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    @Override
    public void reportHeartbeat(HeartbeatMsg heartbeat) {
        httpManager.doSentPost(reportHeartbeatUrl, heartbeat);
    }

    public long getDBSyncInterval() {
        return connInterval + ((long) (random.nextFloat() * connInterval / 2));
    }

    public void reportDBSyncHeartbeat() {
        DbSyncHeartbeatRequest heartbeat = getDbSyncHeartbeat();
        httpManager.doSentPost(dbSyncReportUrl, heartbeat);
    }

    public DbSyncHeartbeatRequest getDbSyncHeartbeat() {
        DbSyncHeartbeatRequest hbRequest = new DbSyncHeartbeatRequest();
        hbRequest.setIp(AgentUtils.getLocalIp());
        List<DbSyncHeartbeat> hbList = new ArrayList<>();
        ConcurrentHashMap<String, DBSyncJob> allJobs = jobManager.getAllJobs();
        // get running job hb
        Set<Map.Entry<String, DBSyncJob>> entrySet = allJobs.entrySet();
        for (Map.Entry<String, DBSyncJob> entry : entrySet) {
            DBSyncJob dbSyncJob = entry.getValue();
            if (dbSyncJob != null) {
                try {
                    DbSyncHeartbeat runningJobHb = dbSyncJob.getReadJob().genHeartBeat(false);
                    if (runningJobHb != null) {
                        hbList.add(runningJobHb);
                    }
                } catch (Exception e) {
                    LOGGER.error("Create dbJobId = {} heartbeat has exception !", dbSyncJob.getDbJobId(), e);
                }
            }
        }
        DbSyncHeartbeat stopHbInfo = null;
        if (!holdStopJobHeartbeats.isEmpty()) {
            do {
                try {
                    stopHbInfo = holdStopJobHeartbeats.poll(1, TimeUnit.MILLISECONDS);
                    if (stopHbInfo != null) {
                        hbList.add(stopHbInfo);
                    }
                } catch (Exception e) {
                    LOGGER.error("Get Hold Heartbeat exception: ", e);
                    break;
                }
            } while (stopHbInfo != null);
        }
        hbRequest.setHeartbeats(hbList);
        return hbRequest;
    }

    /**
     * build heartbeat message of agent
     */
    private HeartbeatMsg buildHeartbeatMsg() {

        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setIp(agentIp);
        heartbeatMsg.setPort(String.valueOf(agentPort));
        heartbeatMsg.setComponentType(ComponentTypeEnum.Agent.getType());
        heartbeatMsg.setReportTime(System.currentTimeMillis());
        if (StringUtils.isNotBlank(clusterName)) {
            heartbeatMsg.setClusterName(clusterName);
        }
        if (StringUtils.isNotBlank(clusterTag)) {
            heartbeatMsg.setClusterTag(clusterTag);
        }
        if (StringUtils.isNotBlank(inCharges)) {
            heartbeatMsg.setInCharges(inCharges);
        }
        if (StringUtils.isNotBlank(nodeGroup)) {
            heartbeatMsg.setNodeGroup(nodeGroup);
        }

        ConcurrentHashMap<String, DBSyncJob> allJobs = jobManager.getAllJobs();
        List<GroupHeartbeat> groupHeartbeats = Lists.newArrayList();
        List<StreamHeartbeat> streamHeartbeats = Lists.newArrayList();
        allJobs.values().forEach(job -> {
            DBSyncJobConf jobProfile = job.getDBSyncJobConf();
            DbAgentReadJob readJob = job.getReadJob();
            State currentState;
            if (readJob != null && readJob.getState() == JobStat.State.RUN) {
                currentState = State.RUNNING;
            } else {
                currentState = State.ACCEPTED;
            }
            Collection<MysqlTableConf> collection = jobProfile.getMysqlTableConfList();
            if (collection != null && collection.size() > 0) {
                for (MysqlTableConf mysqlTableConf : collection) {
                    String status = currentState.name();
                    GroupHeartbeat groupHeartbeat = new GroupHeartbeat();
                    groupHeartbeat.setInlongGroupId(mysqlTableConf.getGroupId());
                    groupHeartbeat.setStatus(status);
                    groupHeartbeats.add(groupHeartbeat);

                    StreamHeartbeat streamHeartbeat = new StreamHeartbeat();
                    streamHeartbeat.setInlongGroupId(mysqlTableConf.getGroupId());
                    streamHeartbeat.setInlongStreamId(mysqlTableConf.getStreamId());
                    streamHeartbeat.setStatus(status);
                    streamHeartbeats.add(streamHeartbeat);
                }
            }
        });
        heartbeatMsg.setGroupHeartbeats(groupHeartbeats);
        heartbeatMsg.setStreamHeartbeats(streamHeartbeats);
        return heartbeatMsg;
    }

    private String buildReportHeartbeatUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_HEARTBEAT_HTTP_PATH, DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH);
    }

    private String buildDBSyncHbUrl() {
        return baseManagerUrl + conf.get(DBSYNC_REPORT_HEARTBEAT, DEFAULT_DBSYNC_REPORT_HEARTBEAT);
    }
}