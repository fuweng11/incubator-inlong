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

package org.apache.inlong.agent.core.ha;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.core.ha.lb.LoadBalanceInfo;
import org.apache.inlong.agent.core.ha.lb.LoadBalanceService;
import org.apache.inlong.agent.core.ha.listener.JobCoordinatorChangeListener;
import org.apache.inlong.agent.core.ha.listener.JobRunNodeChangeListener;
import org.apache.inlong.agent.core.ha.listener.UpdateSwitchNodeChangeListener;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegateImpl;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.inlong.agent.core.ha.zk.ZkUtil;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.core.job.JobManager;
import org.apache.inlong.agent.entites.CommonResponse;
import org.apache.inlong.agent.except.DataSourceConfigException.JobSizeExceedMaxException;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncClusterInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskFullInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.inlong.common.pojo.agent.dbsync.RunningTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_JOB_STATE_MONITOR_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_POSITION_UPDATE_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_HA_RUN_NODE_CHANGE_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MAX_CON_DB_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_SKIP_ZK_POSITION_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_MAX_CON_DB_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_SKIP_ZK_POSITION_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_JOB_STATE_MONITOR_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_POSITION_UPDATE_INTERVAL_MS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_HA_RUN_NODE_CHANGE_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.FetcherConstants.DBSYNC_GET_RUNNING_TASKS;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_DBSYNC_GET_RUNNING_TASKS;

public class JobHaDispatcherImpl implements JobHaDispatcher, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobHaDispatcherImpl.class);
    private static JobHaDispatcherImpl jobHaDispatcher = null;
    /*
     * error task info for addTask, taskId, errorMsg
     */
    private final List<DbSyncTaskInfo> errorTaskInfoList = new ArrayList<>();
    /*
     * error task info for addTask, taskId, errorMsg
     */
    private final List<DbSyncTaskInfo> correctTaskInfoList = new ArrayList<>();
    /*
     * jobs that are exceeds job max size
     */
    private final List<DbSyncTaskInfo> exceedTaskInfoList = new ArrayList<>();
    private final ScheduledExecutorService positionUpdateExecutor;
    private final ScheduledExecutorService jobStateMonitorExecutor;
    private final ProfileFetcher haFetcher;
    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    private String zkUrl;
    private volatile boolean isZkHealth = true;
    /*
     */
    private ConfigDelegate configDelegate;
    /*
     * key : syncId, value : JobHaInfo
     */
    private ConcurrentHashMap<String, JobHaInfo> jobHaInfoMap = new ConcurrentHashMap<>();
    /*
     * local Register Coordinator Path
     */
    private String localRegisterCoordinatorPath;
    private CopyOnWriteArraySet<String> allSyncIdInClusterSet = new CopyOnWriteArraySet<>();
    private CopyOnWriteArraySet<String> needToRunSyncIdSet = new CopyOnWriteArraySet<>();
    private CopyOnWriteArraySet<JobHaInfo> needToStopJobHaInfo = new CopyOnWriteArraySet<>();
    private Integer syncIdListVersion = -1;
    private Integer clusterId;
    private String localIp;
    private JobManager jobManager;
    private int maxSyncIdsThreshold;
    private long coordinatorIntervalMs = 2 * 60 * 1000L;
    private volatile boolean isDbsyncUpdating = false;
    private volatile boolean isJobCoordinator = false;
    private volatile boolean isSkipZkPositionEnable = false;
    private String currentCoordinatorNode;
    private JobCoordinator jobCoordinator = null;
    private JobRunNodeChangeListener jobRunNodeChangeListener;
    private JobCoordinatorChangeListener jobCoordinatorChangeListener;
    private UpdateSwitchNodeChangeListener updateSwitchNodeChangeListener;
    private LoadBalanceService loadBalanceService;
    private ConcurrentHashMap<String, String> jobSenderPosition = new ConcurrentHashMap<>();
    private float haNodeNeedChangeMaxThreshold = 0.7F;
    private float haNodeChangeCandidateThreshold = 0.5F;
    private int loadBalanceCompareLoadUsageThreshold = 10;
    private float loadBalanceCheckLoadThreshold = 0.6F;
    private String getTaskConfigByIpAndServerIdUrl;
    private HttpManager httpManager;
    private Gson gson = new Gson();

    private JobHaDispatcherImpl(JobManager jobManager, ProfileFetcher fetcher) {
        this.getTaskConfigByIpAndServerIdUrl = buildGetRunningTaskUrl();
        this.localIp = AgentUtils.getLocalIp();
        this.isSkipZkPositionEnable = agentConf.getBoolean(DBSYNC_SKIP_ZK_POSITION_ENABLE,
                DEFAULT_DBSYNC_SKIP_ZK_POSITION_ENABLE);
        this.jobManager = jobManager;
        this.haFetcher = fetcher;
        this.jobRunNodeChangeListener = new JobRunNodeChangeListener(this);
        this.jobCoordinatorChangeListener = new JobCoordinatorChangeListener(this);
        this.updateSwitchNodeChangeListener = new UpdateSwitchNodeChangeListener(this);
        this.loadBalanceService = new LoadBalanceService(this, localIp);
        this.coordinatorIntervalMs = agentConf.getLong(DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS,
                DEFAULT_DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS);
        this.maxSyncIdsThreshold = agentConf.getInt(DBSYNC_MAX_CON_DB_SIZE, DEFAULT_DBSYNC_MAX_CON_DB_SIZE);
        this.haNodeChangeCandidateThreshold = agentConf.getFloat(DBSYNC_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD,
                DEFAULT_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD);
        this.haNodeNeedChangeMaxThreshold = agentConf.getFloat(DBSYNC_HA_RUN_NODE_CHANGE_MAX_THRESHOLD,
                DEFAULT_HA_RUN_NODE_CHANGE_MAX_THRESHOLD);
        this.loadBalanceCompareLoadUsageThreshold = agentConf.getInt(DBSYNC_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD,
                DEFAULT_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD);
        this.loadBalanceCheckLoadThreshold =
                agentConf.getFloat(DBSYNC_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD,
                        DEFAULT_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD);
        this.positionUpdateExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("position-updater"));
        this.jobStateMonitorExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("job-state-monitor"));
        long interval = agentConf.getLong(DBSYNC_HA_POSITION_UPDATE_INTERVAL_MS,
                DEFAULT_HA_POSITION_UPDATE_INTERVAL_MS);
        positionUpdateExecutor.scheduleWithFixedDelay(getPositionUpdateTask(),
                interval, interval, TimeUnit.MILLISECONDS);
        interval = agentConf.getLong(DBSYNC_HA_JOB_STATE_MONITOR_INTERVAL_MS, DEFAULT_HA_JOB_STATE_MONITOR_INTERVAL_MS);
        jobStateMonitorExecutor.scheduleWithFixedDelay(getJobStateMonitorTask(),
                interval, interval, TimeUnit.MILLISECONDS);
        this.httpManager = new HttpManager(agentConf);

    }

    public static JobHaDispatcherImpl getInstance(JobManager jobManager, ProfileFetcher fetcher) {
        if (jobHaDispatcher == null) {
            synchronized (JobHaDispatcherImpl.class) {
                if (jobHaDispatcher == null) {
                    jobHaDispatcher = new JobHaDispatcherImpl(jobManager, fetcher);
                }
            }
        }
        return jobHaDispatcher;
    }

    public static JobHaDispatcherImpl getInstance() {
        if (jobHaDispatcher == null) {
            throw new RuntimeException("jobHaDispatcher has not been initialized by HaBinlogFetcher");
        }
        return jobHaDispatcher;
    }

    private String buildGetRunningTaskUrl() {
        return HttpManager.buildBaseUrl() + agentConf.get(DBSYNC_GET_RUNNING_TASKS, DEFAULT_DBSYNC_GET_RUNNING_TASKS);
    }

    /**
     * task config info add to cache
     *
     * @param taskConf
     */
    @Override
    public void addJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("add job syncId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        this.updateJob(taskConf);
    }

    /**
     * start job statue
     *
     * @param taskConf conf
     */
    @Override
    public void startJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("start job syncId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        this.updateJob(taskConf);
    }

    /**
     * update job config info
     *
     * @param taskConf task conf
     */
    @Override
    public void updateJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("update job syncId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        // check haInfo is or not exist
        JobHaInfo jobHaInfo = jobHaInfoMap.computeIfAbsent(taskConf.getServerName(), (k) -> {
            JobHaInfo newJobHaInfo = new JobHaInfo();
            newJobHaInfo.setZkHealth(true);
            String jobInstanceName = taskConf.getDbServerInfo().getUrl() + ":" + taskConf.getServerName();
            newJobHaInfo.setJobName(jobInstanceName);
            newJobHaInfo.setSyncId(k);
            return newJobHaInfo;
        });

        if (jobHaInfo != null) {
            // update config
            jobHaInfo.addTaskConf(taskConf);
        } else {
            LOGGER.error("Task conf is error taskId[{}], syncId[{}]", taskConf.getId(), taskConf.getServerName());
        }
    }

    /**
     * stop job statue
     *
     * @param syncId syncId
     */
    @Override
    public void stopJob(String syncId, Integer taskId) {
        LOGGER.info("stop job syncId[{}], taskId[{}]", syncId, taskId);
        deleteJob(syncId, taskId, null);
    }

    /**
     * stop job
     *
     * @param jobHaInfo
     */
    private void stopJob(JobHaInfo jobHaInfo) {
        String jobName = jobHaInfo.getJobName();
        String syncId = jobHaInfo.getSyncId();
        if (jobHaInfo.getTaskConfMap().size() > 0) {
            Set<Map.Entry<Integer, DbSyncTaskInfo>> entrySet =
                    jobHaInfo.getTaskConfMap().entrySet();
            jobName = null;
            for (Map.Entry<Integer, DbSyncTaskInfo> entry : entrySet) {
                if (jobName == null) {
                    jobName = this.jobManager.getJobNameByTaskId(entry.getValue().getId());
                }
                stopOldRunningNodeTask(jobHaInfo, entry.getValue());
            }
        }
        try {
            if (StringUtils.isNotEmpty(jobName)) {
                this.jobManager.checkAndStopJobForce(jobName);
            }
            String position = jobSenderPosition.remove(syncId);
            if (position != null) {
                jobHaInfo.setSyncPosition(position);
                updatePositionToZk(jobHaInfo, jobHaInfo.getSyncPosition());
            }
            jobHaInfo.getTaskConfMap().clear();
            LOGGER.info("Job syncId[{}] JobName = {} is stopped!", jobHaInfo.getSyncId(), jobName);
        } catch (Throwable e) {
            LOGGER.error("checkAndStopJobForce has error e = {}", e);
        }
    }

    /**
     * delete job config info
     *
     * @param syncId syncId
     */
    @Override
    public void deleteJob(String syncId, Integer taskId, DbSyncTaskInfo taskConfFromTdm) {
        LOGGER.info("delete job syncId[{}], taskId[{}]", syncId, taskId);
        JobHaInfo jobHaInfo = jobHaInfoMap.get(syncId);
        if (jobHaInfo != null) {
            DbSyncTaskInfo taskConf = jobHaInfo.deleteTaskConf(taskId);
            if (taskConf != null) {
                // update version first
                taskConf.setVersion(taskConfFromTdm.getVersion());
                CompletableFuture<Void> opFuture = new CompletableFuture<>();
                jobManager.deleteTask(taskConf, opFuture);
                opFuture.whenComplete((v, e) -> {
                    if (e != null) {
                        LOGGER.error("stop task has error!", e);
                        synchronized (errorTaskInfoList) {
                            errorTaskInfoList.add(taskConf);
                        }
                    } else {
                        LOGGER.info("stop task has completed! syncId[{}], taskId[{}]", syncId, taskId);
                        synchronized (correctTaskInfoList) {
                            correctTaskInfoList.add(taskConf);
                        }
                    }
                });
            } else {
                LOGGER.error("fail to find local taskId[{}], current taskList{}", taskId,
                        jobHaInfo.getTaskConfMap().keys());
                synchronized (errorTaskInfoList) {
                    errorTaskInfoList.add(taskConfFromTdm);
                }

            }
            // delete job when there is no any task
            synchronized (jobHaInfo) {
                if (jobHaInfo.getTaskConfMap().size() <= 0) {
                    jobHaInfoMap.remove(syncId);
                }
            }
        } else {
            LOGGER.info("cannot find local job, delete conf {}", taskConfFromTdm.getId());
            correctTaskInfoList.add(taskConfFromTdm);
        }
    }

    /**
     * start all task once
     *
     * @param taskConfList confList
     */
    @Override
    public List<DBSyncJob> startAllTasksOnce(List<DbSyncTaskInfo> taskConfList) {
        Map<String, DBSyncJob> dbJobsMap = new HashMap<>();
        if (taskConfList != null && taskConfList.size() > 0) {
            HashMap<String, List<DbSyncTaskInfo>> jobGroupMap = new HashMap<String, List<DbSyncTaskInfo>>();
            for (DbSyncTaskInfo conf : taskConfList) {
                List list = jobGroupMap.get(conf.getServerName());
                if (list == null) {
                    list = new ArrayList<DbSyncTaskInfo>();
                    jobGroupMap.put(conf.getServerName(), list);
                }
                list.add(conf);
                LOGGER.info("startAllJobOnce change task syncId[{}] , taskId[{}]",
                        conf.getServerName(), conf.getId());
            }
            Set<Map.Entry<String, List<DbSyncTaskInfo>>> entrySet = jobGroupMap.entrySet();
            for (Map.Entry<String, List<DbSyncTaskInfo>> entry : entrySet) {
                JobHaInfo value = jobHaInfoMap.computeIfPresent(entry.getKey(), (k, jobHaInfo) -> {
                    if (jobHaInfo != null) {
                        List<DbSyncTaskInfo> confList = entry.getValue();
                        LOGGER.info("startJobWithAllTaskOnce syncId[{}], taskSize = {}", jobHaInfo.getSyncId(),
                                (confList == null ? 0 : confList.size()));
                        if (!checkValidConfig(entry, jobHaInfo, confList)) {
                            return jobHaInfo;
                        }
                        String startPositionFromZk = getStartPositionFromZk(jobHaInfo);
                        for (DbSyncTaskInfo taskConf : confList) {
                            LOGGER.info("startJobWithAllTaskOnce add task syncId[{}] "
                                    + ", taskId[{}]",
                                    taskConf.getServerName(), taskConf.getId());
                            CompletableFuture<Void> opFuture = new CompletableFuture<>();
                            initStartPosition(startPositionFromZk, taskConf);
                            jobHaInfo.setSyncPosition(taskConf.getStartPosition());
                            DBSyncJob dbJob = jobManager.addDbSyncTask(taskConf, opFuture);
                            if (dbJob != null) {
                                dbJobsMap.putIfAbsent(jobHaInfo.getSyncId(), dbJob);
                            }
                            LOGGER.info("startJobWithAllTaskOnce add task syncId[{}] "
                                    + ", taskId[{}] finished!",
                                    taskConf.getServerName(), taskConf.getId());
                            opFuture.whenComplete((v, e) -> {
                                if (e != null && !isJobExceedException(e)) {
                                    LOGGER.error("update task has error! e = {}", e);
                                    synchronized (errorTaskInfoList) {
                                        errorTaskInfoList.add(taskConf);
                                    }
                                } else if (isJobExceedException(e)) {
                                    LOGGER.error("job exceed return no error", e);
                                    synchronized (exceedTaskInfoList) {
                                        exceedTaskInfoList.add(taskConf);
                                    }
                                } else {
                                    LOGGER.info("update task has completed! syncId[{}], taskId[{}]",
                                            jobHaInfo.getSyncId(), taskConf.getId());
                                    synchronized (correctTaskInfoList) {
                                        correctTaskInfoList.add(taskConf);
                                    }
                                }
                            });
                        }
                    }
                    return jobHaInfo;
                });
                if (value == null) {
                    LOGGER.warn("jobHaInfoMap {} doesn't contains {}, skip", jobHaInfoMap.keySet(), entry.getKey());
                    continue;
                }
            }
        }
        return dbJobsMap.values().stream().collect(Collectors.toList());
    }

    /**
     * check job exceed exception
     *
     * @param e
     * @return
     */
    private boolean isJobExceedException(Throwable e) {
        return e instanceof JobSizeExceedMaxException;
    }

    private boolean checkValidConfig(Map.Entry<String, List<DbSyncTaskInfo>> entry, JobHaInfo jobHaInfo,
            List<DbSyncTaskInfo> confList) {
        for (DbSyncTaskInfo taskConf : confList) {
            /*
             * when the running node under syncId is removed the jobInfo should also be removed from zk
             */
            if (taskConf.getNodeIps() == null || taskConf.getNodeIps().stream().noneMatch(a -> a.equals(localIp))) {
                LOGGER.info("syncId {}, task Conf {}, doesn't contains localIp {}, so remove "
                        + "candidate and leader node", entry.getKey(), taskConf, localIp);
                return false;
            }
        }
        return true;
    }

    /**
     * initStartPosition
     *
     * @param startPositionFromZk startPositionFromZk
     * @param taskConf taskConf
     * @return
     */
    private boolean initStartPosition(String startPositionFromZk, DbSyncTaskInfo taskConf) {
        String startPosStr = taskConf.getStartPosition();
        if (StringUtils.isEmpty(startPosStr) || DBSyncUtils.isStrNull(startPosStr)) {
            if (!StringUtils.isEmpty(startPositionFromZk) && !isSkipZkPositionEnable) {
                taskConf.setStartPosition(startPositionFromZk);
                LOGGER.info("[{}] startPositionFromZk = {}", taskConf.getServerName(), startPositionFromZk);
            } else {
                LOGGER.warn("[{}/{}] startPositionFromZk is empty Or is skipped! position [{}]",
                        taskConf.getServerName(), isSkipZkPositionEnable, startPositionFromZk);
            }
        } else {
            LOGGER.info("[{}] startPositionFromConf = {}", taskConf.getServerName(), startPosStr);
        }
        return true;
    }

    @Override
    public Set<JobHaInfo> getHaJobInfList() {
        Set<JobHaInfo> set = new HashSet<>();
        set.addAll(jobHaInfoMap.values());
        return set;
    }

    /**
     * update position
     *
     * @param syncId syncId
     * @param position postition
     */
    @Override
    public void updatePosition(String syncId, String position) {
        if (syncId == null) {
            LOGGER.error("[{}] syncId is null!", syncId);
            return;
        }
        jobSenderPosition.put(syncId, position);
        JobHaInfo jobHaInfo = jobHaInfoMap.get(syncId);
        if (jobHaInfo != null) {
            jobHaInfo.setSyncPosition(position);
        } else {
            LOGGER.error("[{}] running job is not exist in HaInfo", syncId);
        }
    }

    /**
     * get position from zk
     *
     * @param jobHaInfo jobHaInfo
     * @return position
     */
    private String getStartPositionFromZk(JobHaInfo jobHaInfo) {
        if (jobHaInfo == null) {
            return null;
        }
        ConfigDelegate configDelegate = getZkConfigDelegate();
        byte[] data = configDelegate.getData(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobPositionPath(String.valueOf(clusterId), jobHaInfo.getSyncId()));
        if (data != null) {
            try {
                return new String(data, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("getStartPositionFromZk e = {}", e);
            }
        }
        return null;
    }

    private void stopOldRunningNodeTask(JobHaInfo jobHaInfo, DbSyncTaskInfo taskConf) {
        LOGGER.info("updateRunNode deleteTask for change runNode info jobHaInfo = {}",
                jobHaInfo);
        CompletableFuture<Void> opFuture = new CompletableFuture<>();
        try {
            jobManager.deleteTask(taskConf, opFuture);
        } catch (Throwable e) {
            LOGGER.error("stop task has error {} syncId[{}], taskId[{}]", e,
                    jobHaInfo.getSyncId(), taskConf.getId());
        }
        try {
            opFuture.get(1, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOGGER.error("stop task has TimeoutException e = {}, syncId[{}], taskId[{}]", e,
                    jobHaInfo.getSyncId(), taskConf.getId());
        } catch (InterruptedException e) {
            LOGGER.error("stop task has InterruptedException e = {}, syncId[{}], taskId[{}]", e,
                    jobHaInfo.getSyncId(), taskConf.getId());
        } catch (ExecutionException e) {
            LOGGER.error("stop task has ExecutionException e = {}, syncId[{}], taskId[{}]", e,
                    jobHaInfo.getSyncId(), taskConf.getId());
        }
    }

    /**
     * update zk stats
     *
     * @param clusterId clusterId
     * @param syncId syncId
     * @param isConnected isConnected
     */
    @Override
    public void updateZkStats(String clusterId, String syncId, boolean isConnected) {
        if (StringUtils.isNotEmpty(syncId)) {
            JobHaInfo jobHaInfo = jobHaInfoMap.get(syncId);
            if (jobHaInfo != null && !jobHaInfo.isZkHealth() && isConnected) {
                initSyncInfoToZk(zkUrl, jobHaInfo.getSyncId(), clusterId);
            }
            jobHaInfo.setZkHealth(isConnected);
        }

        if (StringUtils.isNotEmpty(clusterId)) {
            if (!isZkHealth && isConnected) {
                initClusterInfoToZk(Integer.parseInt(clusterId));
            }
            isZkHealth = isConnected;
        }
    }

    @Override
    public List<DbSyncTaskInfo> getErrorTaskConfInfList() {
        List list = null;
        if (errorTaskInfoList.size() > 0) {
            list = new ArrayList();
            synchronized (errorTaskInfoList) {
                list.addAll(errorTaskInfoList);
                errorTaskInfoList.clear();
            }
        }
        return list;
    }

    @Override
    public List<DbSyncTaskInfo> getExceedTaskInfList() {
        List<DbSyncTaskInfo> list = null;
        if (exceedTaskInfoList.size() > 0) {
            list = new ArrayList();
            synchronized (exceedTaskInfoList) {
                list.addAll(exceedTaskInfoList);
                exceedTaskInfoList.clear();
            }
        }
        return list;
    }

    public List<DbSyncTaskInfo> getCorrectTaskConfInfList() {
        List<DbSyncTaskInfo> list = null;
        if (correctTaskInfoList.size() > 0) {
            list = new ArrayList();
            synchronized (correctTaskInfoList) {
                list.addAll(correctTaskInfoList);
                correctTaskInfoList.clear();
            }
        }
        return list;
    }

    public void close() throws Exception {
        loadBalanceService.close();
        if (configDelegate != null) {
            try {
                configDelegate.close();
            } catch (Exception exception) {
                LOGGER.error("zkConfigDelegateMap close has exception = {}", exception);
            }
        }
    }

    private void updatePositionToZk(JobHaInfo jobHaInfo, String position) {
        if (StringUtils.isEmpty(position)) {
            return;
        }
        try {
            ConfigDelegate configDelegate = getZkConfigDelegate();
            configDelegate.createPathAndSetData(ConfigDelegate.ZK_GROUP,
                    ZkUtil.getJobPositionPath(String.valueOf(clusterId),
                            jobHaInfo.getSyncId()),
                    position);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] updatePosition {} to zk!", jobHaInfo.getSyncId(), position);
            }

        } catch (Throwable e) {
            LOGGER.error("updatePositionToZk has exception ", e);
        }
    }

    /**
     * ha dispatch node to stop run job
     *
     * @param syncId syncId
     */
    @Override
    public void removeLocalRunJob(String syncId) {
        if (needToRunSyncIdSet.remove(syncId)) {
            LOGGER.info("remove syncId [{}] form local set!", syncId);
        }
    }

    /**
     * ha dispatch node to run job
     *
     * @param syncId job syncId
     * @param jobRunNodeInfo job run node info
     */
    @Override
    public void updateRunJobInfo(String syncId, JobRunNodeInfo jobRunNodeInfo) {
        if (jobRunNodeInfo != null && localIp.equals(jobRunNodeInfo.getIp())) {
            if (needToRunSyncIdSet.add(syncId)) {
                LOGGER.info("add syncId [{}] to local set!", syncId);
            }
        } else {
            if (needToRunSyncIdSet.remove(syncId)) {
                LOGGER.info("remove syncId [{}] form local set!", syncId);
            }
        }
        if (isJobCoordinator && jobCoordinator != null) {
            jobCoordinator.setNeedUpdateRunNodeMap(true);
        }
    }

    /**
     * update coordinator
     *
     * @param clusterId clusterId
     * @param changeCoordinatorPath changeCoordinatorPath
     */
    @Override
    public void updateJobCoordinator(String clusterId, String changeCoordinatorPath) {
        if (clusterId == null) {
            return;
        }
        String jobCoordinatorParentPath = ZkUtil.getCoordinatorParentPath(clusterId);
        ConfigDelegate configDelegate = getZkConfigDelegate();
        List<String> paths = configDelegate.getChildren(ConfigDelegate.ZK_GROUP,
                jobCoordinatorParentPath);
        Collections.sort(paths, new Comparator<String>() {

            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        if (paths != null && paths.size() > 0) {
            if (paths.get(0).equals(localRegisterCoordinatorPath)) {
                this.isJobCoordinator = true;
            } else {
                this.isJobCoordinator = false;
            }
            currentCoordinatorNode = paths.get(0);
        } else {
            this.isJobCoordinator = false;
        }
        LOGGER.info("updateJobCoordinator isJobCoordinator [{}], currentCoordinatorNode [{}]",
                isJobCoordinator, currentCoordinatorNode);
    }

    @Override
    public boolean updateSyncIdList(Integer newClusterId, Integer syncIdListVersion,
            String zkServer, List<String> syncIdList, boolean needCheckClusterIdOrSyncIdListVersion) {
        if (newClusterId == null || StringUtils.isEmpty(zkServer)) {
            LOGGER.error("newClusterId = {}, zkServer = {} has error!", newClusterId, zkServer);
            return false;
        }
        if ((newClusterId != clusterId)
                || (zkUrl == null || !zkUrl.equals(zkServer))) {
            ConfigDelegate configDelegate = this.configDelegate;
            /*
             * clean old zk client
             */
            if (configDelegate != null) {
                try {
                    /*
                     * clean listener and emp node
                     */
                    configDelegate.close();
                } catch (Exception e) {
                    LOGGER.error("Close zk client has Exception e = {}", e);
                }
            }
            LOGGER.info("change zkUrl Info old = {} , new = {}", zkUrl, zkServer);
            zkUrl = zkServer;
            initClusterInfoToZk(newClusterId);
            loadBalanceService.setClusterId(String.valueOf(newClusterId));
        }

        if (clusterId != newClusterId || this.syncIdListVersion != syncIdListVersion
                || !needCheckClusterIdOrSyncIdListVersion) {
            Set<String> currentSyncIdList = this.allSyncIdInClusterSet;
            /*
             * clean old
             */
            List cleanSyncIdList = null;
            for (String serverName : currentSyncIdList) {
                if (!syncIdList.contains(serverName)) {
                    if (cleanSyncIdList == null) {
                        cleanSyncIdList = new ArrayList<>();
                    }
                    cleanSyncIdList.add(serverName);
                }
            }
            if (cleanSyncIdList != null) {
                this.allSyncIdInClusterSet.removeAll(cleanSyncIdList);
                cleanMovedJobInfo(cleanSyncIdList, zkServer);
            }
            LOGGER.info("update clusterId {}/{} or syncIdListVersion {}/{}!", clusterId,
                    newClusterId, this.syncIdListVersion, syncIdListVersion);
            /*
             * register new info to zk
             */
            if (syncIdList != null && syncIdList.size() > 0) {
                for (String syncId : syncIdList) {
                    if (!this.allSyncIdInClusterSet.contains(syncId)) {
                        initSyncInfoToZk(zkServer, syncId, String.valueOf(newClusterId));
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("allSyncIdInClusterSet add syncId {}!", syncId);
                        }

                        this.allSyncIdInClusterSet.add(syncId);
                    }
                }
            }
            this.clusterId = newClusterId;
            this.syncIdListVersion = syncIdListVersion;
        }
        return true;
    }

    @Override
    public void updateIsUpdating() {
        if (configDelegate != null) {
            byte[] data = configDelegate.getData(ConfigDelegate.ZK_GROUP,
                    Constants.SYNC_JOB_UPDATE_SWITCH_PATH);
            try {
                String switchInfo = new String(data, "UTF-8");
                if (StringUtils.isNotEmpty(switchInfo)) {
                    UpdateSwitchInfo updateSwitchInfo = gson.fromJson(switchInfo, UpdateSwitchInfo.class);
                    isDbsyncUpdating = updateSwitchInfo == null ? false : updateSwitchInfo.isUpdating();
                } else {
                    isDbsyncUpdating = false;
                }
            } catch (Exception e) {
                LOGGER.error("UpdateSwitch has exception e = {}", e);
            }
        }
        LOGGER.info("updateIsUpdating isDbsyncUpdating = {}", isDbsyncUpdating);
    }

    private boolean initClusterInfoToZk(Integer newClusterId) {
        if (newClusterId != clusterId) {

            if (StringUtils.isEmpty(zkUrl) || newClusterId == null) {
                LOGGER.warn("zkServer = {}, clusterId = {} params has error", zkUrl,
                        newClusterId);
                return false;
            }
            LOGGER.info("initClusterInfoToZk zkServer = {}, clusterId = {}", zkUrl, newClusterId);
            ConfigDelegate configDelegate = getZkConfigDelegate();
            /*
             * create new one
             */
            /*
             * add listener 1-1
             */
            String coordinatorNodeParentPath =
                    ZkUtil.getCoordinatorParentPath(String.valueOf(newClusterId));
            configDelegate.removeChildNodeListener(ConfigDelegate.ZK_GROUP, coordinatorNodeParentPath);
            configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP, coordinatorNodeParentPath);
            configDelegate.addChildNodeListener(jobCoordinatorChangeListener,
                    ConfigDelegate.ZK_GROUP, coordinatorNodeParentPath);

            /*
             * register for coordinator node
             */
            String coordinatorPath =
                    configDelegate.createOrderEphemeralPathAndSetData(ConfigDelegate.ZK_GROUP,
                            ZkUtil.getZkPath(coordinatorNodeParentPath, Constants.ZK_FOR_LEADER), localIp);
            localRegisterCoordinatorPath = ZkUtil.getLastNodePath(coordinatorPath);

            /*
             * create candidate path and register
             */
            String jobCandidateParentPath =
                    ZkUtil.getCandidateParentPath(String.valueOf(newClusterId));
            LoadBalanceInfo initLoadBalanceInfo = new LoadBalanceInfo();
            initLoadBalanceInfo.setIp(localIp);
            initLoadBalanceInfo.setMaxSyncIdsThreshold(maxSyncIdsThreshold);
            configDelegate.createEphemeralPathAndSetData(ConfigDelegate.ZK_GROUP,
                    ZkUtil.getZkPath(jobCandidateParentPath, localIp),
                    gson.toJson(initLoadBalanceInfo));

            /*
             * watch updating switch
             */
            configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP,
                    Constants.SYNC_JOB_UPDATE_SWITCH_PATH);
            configDelegate.removeNodeListener(ConfigDelegate.ZK_GROUP, Constants.SYNC_JOB_UPDATE_SWITCH_PATH);
            configDelegate.addNodeListener(updateSwitchNodeChangeListener,
                    ConfigDelegate.ZK_GROUP, Constants.SYNC_JOB_UPDATE_SWITCH_PATH);
            updateIsUpdating();
            return true;
        }
        return true;
    }

    /**
     * init and regist syncid to zk
     *
     * @param zkServer
     * @param syncId
     * @return
     */
    private boolean initSyncInfoToZk(String zkServer, String syncId, String clusterId) {
        if (StringUtils.isEmpty(zkServer) || StringUtils.isEmpty(syncId)
                || StringUtils.isEmpty(clusterId)) {
            LOGGER.warn("zkServer = {}, synId = {}, clusterId = {} params has error",
                    zkServer, syncId, clusterId);
            return false;
        }
        LOGGER.info("initInfoToZk zkServer = {}, synId = {}", zkServer, syncId);
        ConfigDelegate configDelegate = getZkConfigDelegate();

        /*
         * 1 create job run node info
         */
        configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobRunNodePath(clusterId, syncId));

        /*
         * 2 create position node
         */
        configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobPositionPath(clusterId, syncId));

        String jobPath = ZkUtil.getJobRunNodePath(clusterId, syncId);
        configDelegate.removeNodeListener(ConfigDelegate.ZK_GROUP, jobPath);
        configDelegate.addNodeListener(jobRunNodeChangeListener, ConfigDelegate.ZK_GROUP, jobPath);

        return true;
    }

    private boolean cleanSyncInfoFromZk(String zkServer, String syncId) {
        if (StringUtils.isEmpty(zkServer) || StringUtils.isEmpty(syncId)) {
            LOGGER.warn("zkServer = {}, synId = {} params has error", zkServer, syncId);
            return false;
        }
        LOGGER.info("cleanSyncInfoFromZk zkServer = {}, synId = {}", zkServer, syncId);
        ConfigDelegate configDelegate = getZkConfigDelegate();
        configDelegate.removeNodeListener(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobRunNodePath(String.valueOf(clusterId), syncId));
        return false;
    }

    private void cleanMovedJobInfo(List<Integer> serverIdList, String zkUrl) {
        if (serverIdList != null) {
            for (Integer syncId : serverIdList) {
                JobHaInfo jobHaInfo = jobHaInfoMap.remove(String.valueOf(syncId));
                if (jobHaInfo != null) {
                    stopJob(jobHaInfo);
                }
                cleanSyncInfoFromZk(zkUrl, String.valueOf(syncId));
            }
        }
    }

    public void init() {

    }

    @Override
    public Integer getClusterId() {
        return clusterId;
    }

    public void setClusterId(Integer clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public Integer getSyncIdListVersion() {
        return syncIdListVersion;
    }

    @Override
    public Set<String> getNeedToRunSyncIdSet() {
        return needToRunSyncIdSet;
    }

    @Override
    public boolean isDbsyncUpdating() {
        return isDbsyncUpdating;
    }

    @Override
    public boolean isCoordinator() {
        return isJobCoordinator;
    }

    @Override
    public int getMaxSyncIdsThreshold() {
        return maxSyncIdsThreshold;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    /**
     * get zk configDelegate
     *
     * @return ConfigDelegateImpl
     */
    @Override
    public ConfigDelegate getZkConfigDelegate() {
        if (StringUtils.isEmpty(zkUrl)) {
            return null;
        }
        if (configDelegate == null) {
            Map<String, String> connectStrMap = new HashMap<>();
            connectStrMap.put(ConfigDelegate.ZK_GROUP, zkUrl);
            configDelegate = new ConfigDelegateImpl(connectStrMap);
        }
        return configDelegate;
    }

    private CompletableFuture addSyncIdAsync(String syncId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                initTaskInfoByServerId(syncId);
            } catch (Exception e) {
                LOGGER.error("initTaskInfoByServerId syncId[{}] has exception", syncId, e);
            }
            return true;
        }).thenAccept(u -> {
            LOGGER.info("addSyncIdAsync syncId[{}] finished", syncId);
        }).exceptionally(ex -> {
            LOGGER.error("addSyncIdAsync syncId[{}] finished has exception", syncId, ex);
            return null;
        });
    }

    /**
     * get all taskInfos of one server Id for run jobs
     *
     * @param syncId syncId
     */
    private int initTaskInfoByServerId(String syncId) throws Exception {
        int taskConfigSize = -1;
        String clusterName = agentConf.get(AGENT_CLUSTER_NAME);
        String clusterTag = agentConf.get(AGENT_CLUSTER_TAG);
        DbSyncClusterInfo clusterInfo = new DbSyncClusterInfo(getClusterId(), clusterName, getSyncIdListVersion());
        RunningTaskRequest runningTaskRequest = new RunningTaskRequest(localIp, clusterTag, clusterName, clusterInfo,
                syncId);

        String jobConfigString = httpManager.doSentPost(getTaskConfigByIpAndServerIdUrl, runningTaskRequest);
        CommonResponse<DbSyncTaskFullInfo> commonResponse =
                CommonResponse.fromJson(jobConfigString, DbSyncTaskFullInfo.class);
        if (commonResponse == null || !haFetcher.parseJobAndCheckForStart(commonResponse)) {
            throw new Exception("get task's configs has error by server Id!");
        }
        List<DbSyncTaskInfo> data = commonResponse.getData().getTaskInfoList();
        if (data != null) {
            taskConfigSize = data.size();
        }
        LOGGER.info("change run node Id  syncId[{}] , [{}], taskConfigSize = {}", syncId,
                getTaskConfigByIpAndServerIdUrl, taskConfigSize);
        return taskConfigSize;
    }

    private CompletableFuture removeSyncIdAsync(JobHaInfo jobHaInfo) {
        if (jobHaInfo == null) {
            return null;
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                stopJob(jobHaInfo);
            } catch (Exception e) {
                LOGGER.error("stop job by syncId[{}] has exception e = {}", jobHaInfo.getSyncId(), e);
            }
            return true;
        }).thenAccept(u -> {
            needToStopJobHaInfo.remove(jobHaInfo);
            jobHaInfoMap.remove(jobHaInfo.getSyncId());
            LOGGER.info("stop job syncId[{}] finished!", jobHaInfo.getSyncId());
        }).exceptionally(ex -> {
            LOGGER.error("stop job syncId[{}] finished has exception e = {}", jobHaInfo.getSyncId(), ex);
            return null;
        });
    }

    private Runnable getPositionUpdateTask() {
        return () -> {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("PositionUpdateTask is running! report size [{}]",
                            jobSenderPosition.size());
                }

                jobSenderPosition.forEach((k, v) -> {
                    JobHaInfo jobHaInfo = jobHaInfoMap.get(k);
                    if (jobHaInfo != null) {
                        updatePositionToZk(jobHaInfo, v);
                    } else {
                        LOGGER.warn("[{}] running job is not exist in HaInfo", k);
                    }
                });
            } catch (Throwable e) {
                LOGGER.error("getPositionUpdateTask has exception ", e);
            }
        };
    }

    private Runnable getJobStateMonitorTask() {
        return () -> {
            try {
                /*
                 * monitor for coordinate state
                 */
                if (isJobCoordinator && clusterId != null) {
                    Integer currentClusterId = clusterId;
                    if (jobCoordinator == null) {
                        jobCoordinator = new JobCoordinator(currentClusterId,
                                coordinatorIntervalMs, this,
                                haNodeNeedChangeMaxThreshold,
                                haNodeChangeCandidateThreshold, loadBalanceCheckLoadThreshold,
                                loadBalanceCompareLoadUsageThreshold);
                        jobCoordinator.start();
                        LOGGER.info("[{}] jobCoordinator is started!", clusterId);
                    } else if (!clusterId.equals(jobCoordinator.getClusterId())) {
                        jobCoordinator.stop();
                        LOGGER.info("[{}] jobCoordinator is stopped!",
                                jobCoordinator.getClusterId());
                        jobCoordinator = new JobCoordinator(currentClusterId,
                                coordinatorIntervalMs, this,
                                haNodeNeedChangeMaxThreshold,
                                haNodeChangeCandidateThreshold, loadBalanceCheckLoadThreshold,
                                loadBalanceCompareLoadUsageThreshold);
                        jobCoordinator.start();
                        LOGGER.info("[{}] jobCoordinator is started!", clusterId);
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("[{}] jobCoordinator is started!", clusterId);
                        }

                    }
                    jobCoordinator.updateServerIDList(currentClusterId, this.syncIdListVersion,
                            allSyncIdInClusterSet);
                } else if (jobCoordinator != null) {
                    jobCoordinator.stop();
                    LOGGER.info("[{}] jobCoordinator is stopped!", jobCoordinator.getClusterId());
                    jobCoordinator = null;
                }
                LOGGER.info("JobCoordinator currentCoordinatorNode is {}", currentCoordinatorNode);

                /*
                 * monitor for job run state
                 */
                /*
                 * old syncId in needToRunSyncIdSet but has been removed, and need to be stop!
                 */
                jobHaInfoMap.forEach((syncId, jobHaInfo) -> {
                    if (!needToRunSyncIdSet.contains(syncId)) {
                        jobHaInfo = jobHaInfoMap.get(syncId);
                        if (jobHaInfo != null) {
                            needToStopJobHaInfo.add(jobHaInfo);
                        }
                    }
                });

                /*
                 * start stop
                 */
                List<CompletableFuture> futures = needToStopJobHaInfo.stream().map((jobHaInfo) -> {
                    LOGGER.info("Monitor stop syncId[{}]!", jobHaInfo.getSyncId());
                    CompletableFuture re = removeSyncIdAsync(jobHaInfo);
                    LOGGER.info("Monitor old syncId[{}] is stopped!", jobHaInfo.getSyncId());
                    return re;
                }).collect(Collectors.toList());
                CompletableFuture allOf =
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                allOf.join();

                /*
                 * new syncId in needToRunSyncIdSet but not start run
                 */
                futures = needToRunSyncIdSet.stream().map((syncId) -> {
                    JobHaInfo jobHaInfo = jobHaInfoMap.get(syncId);
                    if (jobHaInfo == null || !jobManager.isRunningJob(syncId)) {
                        LOGGER.info("Monitor start new syncId[{}]", syncId);
                        CompletableFuture re = addSyncIdAsync(syncId);
                        LOGGER.info("Monitor new syncId[{}] is started", syncId);
                        return re;
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Monitor syncId[{}] is running now!", syncId);
                        }
                    }
                    return null;
                }).filter((f) -> f != null).collect(Collectors.toList());
                allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                allOf.join();
            } catch (Throwable e) {
                LOGGER.error("getPositionUpdateTask has exception ", e);
            }
        };
    }
}