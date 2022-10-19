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

package org.apache.inlong.agent.core;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.core.job.Job;
import org.apache.inlong.agent.core.job.JobManager;
import org.apache.inlong.agent.core.job.JobWrapper;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.common.heartbeat.AbstractHeartbeatManager;
import org.apache.inlong.common.heartbeat.GroupHeartbeat;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.common.heartbeat.StreamHeartbeat;
import org.apache.inlong.common.pojo.agent.TaskSnapshotMessage;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeatRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_IN_CHARGES;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_HTTP_PORT;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HEART_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MANAGER_AUTH_TOKEN;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MANAGER_SERVICE_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_AGENT_HTTP_PORT;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_HEART_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_MANAGER_AUTH_TOKEN;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_MANAGER_SERVICE_NAME;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_HEARTBEAT_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_HEARTBEAT_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_REPORTSNAPSHOT_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DBSYNC_REPORT_HEARTBEAT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_HEARTBEAT_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_REPORTSNAPSHOT_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_DBSYNC_REPORT_HEARTBEAT;
import static org.apache.inlong.agent.constant.JobConstants.JOB_GROUP_ID;
import static org.apache.inlong.agent.constant.JobConstants.JOB_STREAM_ID;

/**
 * report heartbeat to inlong-manager
 */
public class HeartbeatManager extends AbstractDaemon implements AbstractHeartbeatManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatManager.class);

    private final AgentManager agentManager;
    private final JobManager jobmanager;
    private final AgentConfiguration conf;
    private final HttpManager httpManager;
    private final String baseManagerUrl;
    private final String reportSnapshotUrl;
    private final String reportHeartbeatUrl;
    private final String dbSyncReportUrl;
    private final Pattern numberPattern = Pattern.compile("^[-+]?[\\d]*$");

    private LinkedBlockingQueue<JobAlarmMsg> alarmMsg;
    private LinkedBlockingQueue<JSONObject> holdHeartbeats;
    private ConcurrentHashMap<String, DBSyncJob> monitorJobs;
    private ConcurrentHashMap<String, Long> jobHb;
    private ConcurrentHashMap<String, LogPosition> jobSenderPosition;
    private ConcurrentHashMap<String, LogPosition> jobNewestPosition;
    private long connInterval;
    private Random random = new Random();

    /**
     * Init heartbeat manager.
     */
    public HeartbeatManager(AgentManager agentManager) {
        this.conf = AgentConfiguration.getAgentConf();
        this.agentManager = agentManager;
        connInterval = conf.getLong(DBSYNC_HEART_INTERVAL, DEFAULT_DBSYNC_HEART_INTERVAL);
        jobmanager = agentManager.getJobManager();
        httpManager = new HttpManager(conf);
        baseManagerUrl = HttpManager.buildBaseUrl();
        reportSnapshotUrl = buildReportSnapShotUrl(baseManagerUrl);
        reportHeartbeatUrl = buildReportHeartbeatUrl(baseManagerUrl);
        dbSyncReportUrl = buildDBSyncHbUrl();
    }

    @Override
    public void start() throws Exception {
        submitWorker(snapshotReportThread());
        submitWorker(heartbeatReportThread());
    }

    private Runnable snapshotReportThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    TaskSnapshotRequest taskSnapshotRequest = buildTaskSnapshotRequest();
                    httpManager.doSentPost(reportSnapshotUrl, taskSnapshotRequest, "", "");
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(" {} report to manager", taskSnapshotRequest);
                    }
                    SECONDS.sleep(conf.getInt(AGENT_HEARTBEAT_INTERVAL, DEFAULT_AGENT_HEARTBEAT_INTERVAL));
                } catch (Throwable e) {
                    LOGGER.error("interrupted while report snapshot", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                }
            }
        };
    }

    private Runnable heartbeatReportThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    if (conf.enableHA()) {
                        reportDBSyncHeartbeat();
                        TimeUnit.MILLISECONDS.sleep(getDBSyncInterval());
                    } else {
                        reportHeartbeat(buildHeartbeatMsg());
                        SECONDS.sleep(heartbeatInterval());
                    }

                } catch (Throwable e) {
                    LOGGER.error("interrupted while report heartbeat", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
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
        httpManager.doSentPost(reportHeartbeatUrl, heartbeat, "", "");
    }

    public long getDBSyncInterval() {
        return connInterval + ((long) (random.nextFloat() * connInterval / 2));
    }

    public void reportDBSyncHeartbeat() {
        DbSyncHeartbeatRequest heartbeat = getDbSyncHeartbeat();
        String token = conf.get(DBSYNC_MANAGER_AUTH_TOKEN, DEFAULT_DBSYNC_MANAGER_AUTH_TOKEN);
        String serviceName = conf.get(DBSYNC_MANAGER_SERVICE_NAME, DEFAULT_DBSYNC_MANAGER_SERVICE_NAME);
        httpManager.doSentPost(dbSyncReportUrl, heartbeat, token, serviceName);
    }

    public DbSyncHeartbeatRequest getDbSyncHeartbeat() {
        List<DbSyncHeartbeat> hbList = new ArrayList<>();
        DbSyncHeartbeatRequest hbRequest = new DbSyncHeartbeatRequest();
        hbRequest.setIp(AgentUtils.fetchLocalIp());

        //TODO:add stopJob hbInfo

        //TODO: add runningjob hbInfo

        hbRequest.setIp(AgentUtils.getLocalIp());
        hbRequest.setHeartbeats(hbList);

        return hbRequest;
    }

    public void initMonitor(String jobName, long hbTimeStample, DBSyncJob job) {
        LOGGER.info("Monitor job add {}", jobName);
        monitorJobs.put(jobName, job);
        jobHb.put(jobName, hbTimeStample);
    }

    public void removeJobMonitor(String jobName) {
        LOGGER.info("remove job monitor {}", jobName);
        monitorJobs.remove(jobName);
        jobHb.remove(jobName);
        jobSenderPosition.remove(jobName);
    }

    /**
     * build task snapshot request of job
     */
    private TaskSnapshotRequest buildTaskSnapshotRequest() {
        Map<String, JobWrapper> jobWrapperMap = jobmanager.getJobs();
        List<TaskSnapshotMessage> taskSnapshotMessageList = new ArrayList<>();
        TaskSnapshotRequest taskSnapshotRequest = new TaskSnapshotRequest();

        Date date = new Date(System.currentTimeMillis());
        for (Map.Entry<String, JobWrapper> entry : jobWrapperMap.entrySet()) {
            if (StringUtils.isBlank(entry.getKey()) || entry.getValue() == null) {
                LOGGER.info("key: {} or value: {} is null", entry.getKey(), entry.getValue());
                continue;
            }
            String offset = entry.getValue().getSnapshot();
            String jobId = entry.getKey();
            TaskSnapshotMessage snapshotMessage = new TaskSnapshotMessage();
            snapshotMessage.setSnapshot(offset);

            // TODO Need to make sure the jobId is an Integer
            if (!numberPattern.matcher(jobId).matches()) {
                continue;
            }
            snapshotMessage.setJobId(Integer.valueOf(jobId));
            taskSnapshotMessageList.add(snapshotMessage);
        }
        taskSnapshotRequest.setSnapshotList(taskSnapshotMessageList);
        taskSnapshotRequest.setReportTime(date);
        taskSnapshotRequest.setAgentIp(AgentUtils.fetchLocalIp());
        taskSnapshotRequest.setUuid(AgentUtils.fetchLocalUuid());
        return taskSnapshotRequest;
    }

    /**
     * build heartbeat message of agent
     */
    private HeartbeatMsg buildHeartbeatMsg() {
        final String agentIp = AgentUtils.fetchLocalIp();
        final int agentPort = conf.getInt(AGENT_HTTP_PORT, DEFAULT_AGENT_HTTP_PORT);
        final String clusterName = conf.get(AGENT_CLUSTER_NAME);
        final String clusterTag = conf.get(AGENT_CLUSTER_TAG);
        final String inCharges = conf.get(AGENT_CLUSTER_IN_CHARGES);

        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setIp(agentIp);
        heartbeatMsg.setPort(String.valueOf(agentPort));
        heartbeatMsg.setComponentType(ComponentTypeEnum.Agent.getName());
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

        Map<String, JobWrapper> jobWrapperMap = jobmanager.getJobs();
        List<GroupHeartbeat> groupHeartbeats = Lists.newArrayList();
        List<StreamHeartbeat> streamHeartbeats = Lists.newArrayList();
        jobWrapperMap.values().forEach(jobWrapper -> {
            Job job = jobWrapper.getJob();
            JobProfile jobProfile = job.getJobConf();
            final String groupId = jobProfile.get(JOB_GROUP_ID);
            final String streamId = jobProfile.get(JOB_STREAM_ID);
            State currentState = jobWrapper.getCurrentState();
            String status = currentState.name();
            GroupHeartbeat groupHeartbeat = new GroupHeartbeat();
            groupHeartbeat.setInlongGroupId(groupId);
            groupHeartbeat.setStatus(status);
            groupHeartbeats.add(groupHeartbeat);

            StreamHeartbeat streamHeartbeat = new StreamHeartbeat();
            streamHeartbeat.setInlongGroupId(groupId);
            streamHeartbeat.setInlongStreamId(streamId);
            streamHeartbeat.setStatus(status);
            streamHeartbeats.add(streamHeartbeat);
        });

        heartbeatMsg.setGroupHeartbeats(groupHeartbeats);
        heartbeatMsg.setStreamHeartbeats(streamHeartbeats);
        return heartbeatMsg;
    }

    private String buildReportSnapShotUrl(String baseUrl) {
        return baseUrl
                + conf.get(AGENT_MANAGER_REPORTSNAPSHOT_HTTP_PATH, DEFAULT_AGENT_MANAGER_REPORTSNAPSHOT_HTTP_PATH);
    }

    private String buildReportHeartbeatUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_HEARTBEAT_HTTP_PATH, DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH);
    }

    private String buildDBSyncHbUrl() {
        return baseManagerUrl + conf.get(DBSYNC_REPORT_HEARTBEAT, DEFAULT_DBSYNC_REPORT_HEARTBEAT);
    }

    private static class JobAlarmMsg {

        String jobName;
        String msg;

        public JobAlarmMsg(String jobName, String msg) {
            this.jobName = jobName;
            this.msg = msg;
        }
    }
}