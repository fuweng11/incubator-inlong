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

package org.apache.inlong.agent.core.dbsync.ha;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.core.dbsync.DBSyncJob;
import org.apache.inlong.agent.core.dbsync.DbAgentJobManager;
import org.apache.inlong.agent.core.dbsync.IDbAgentProfileFetcher;
import org.apache.inlong.agent.core.dbsync.ha.lb.LoadBalanceInfo;
import org.apache.inlong.agent.core.dbsync.ha.lb.LoadBalanceService;
import org.apache.inlong.agent.core.dbsync.ha.listener.JobCoordinatorChangeListener;
import org.apache.inlong.agent.core.dbsync.ha.listener.JobRunNodeChangeListener;
import org.apache.inlong.agent.core.dbsync.ha.listener.UpdateSwitchNodeChangeListener;
import org.apache.inlong.agent.core.dbsync.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.dbsync.ha.zk.ConfigDelegateImpl;
import org.apache.inlong.agent.core.dbsync.ha.zk.Constants;
import org.apache.inlong.agent.core.dbsync.ha.zk.ZkUtil;
import org.apache.inlong.agent.except.DataSourceConfigException.JobSizeExceedMaxException;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.MonitorLogUtils;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
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

import static org.apache.inlong.agent.conf.DBSyncJobConf.RUNNING_MODEL_STOPPED;
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

public class JobHaDispatcherImpl implements JobHaDispatcher {

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
    private final IDbAgentProfileFetcher haFetcher;
    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    private String zkUrl;
    private volatile boolean isZkHealth = true;
    /*
     */
    private ConfigDelegate configDelegate;
    /*
     * key : dbJobId, value : JobHaInfo
     */
    private ConcurrentHashMap<String, JobHaInfo> jobHaInfoMap = new ConcurrentHashMap<>();
    /*
     * local Register Coordinator Path
     */
    private String localRegisterCoordinatorPath;
    private CopyOnWriteArraySet<String> allDbJobIdClusterSet = new CopyOnWriteArraySet<>();
    private CopyOnWriteArraySet<String> needToRunDbJobIdSet = new CopyOnWriteArraySet<>();
    private CopyOnWriteArraySet<JobHaInfo> needToStopJobHaInfo = new CopyOnWriteArraySet<>();
    private Integer dbJobIdListVersion = -1;
    private Integer clusterId;
    private String localIp;
    private String registerKey;
    private DbAgentJobManager jobManager;
    private int maxDbJobsThreshold;
    private long coordinatorIntervalMs = 2 * 60 * 1000L;
    private volatile boolean isDbsyncUpdating = false;
    private volatile boolean isJobCoordinator = false;
    private volatile boolean isNeedRetryRegisterLocalCoordinator = false;
    private volatile boolean isSkipZkPositionEnable = false;
    private String currentCoordinatorNode;
    private volatile String jobCoordinatorIp;
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
    private volatile boolean running = true;
    private long lastPrintUpdatePositionTimestamp = 0L;
    private long printUpdatePositionInterval = 5 * 60 * 1000L;
    private Gson gson = new Gson();

    private JobHaDispatcherImpl(DbAgentJobManager jobManager, ProfileFetcher fetcher) {
        this.isSkipZkPositionEnable = agentConf.getBoolean(DBSYNC_SKIP_ZK_POSITION_ENABLE,
                DEFAULT_DBSYNC_SKIP_ZK_POSITION_ENABLE);
        this.jobManager = jobManager;
        if (fetcher != null && fetcher instanceof IDbAgentProfileFetcher) {
            this.haFetcher = (IDbAgentProfileFetcher) fetcher;
            this.localIp = haFetcher.getLocalIp();
            this.registerKey = haFetcher.getRegisterKey();
        } else {
            this.haFetcher = null;
            this.localIp = AgentUtils.getLocalIp();
            this.registerKey = localIp + ":" + "1";
        }

        this.jobRunNodeChangeListener = new JobRunNodeChangeListener(this);
        this.jobCoordinatorChangeListener = new JobCoordinatorChangeListener(this);
        this.updateSwitchNodeChangeListener = new UpdateSwitchNodeChangeListener(this);
        this.loadBalanceService = new LoadBalanceService(this, registerKey);
        this.coordinatorIntervalMs = agentConf.getLong(DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS,
                DEFAULT_DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS);
        this.maxDbJobsThreshold = agentConf.getInt(DBSYNC_MAX_CON_DB_SIZE, DEFAULT_DBSYNC_MAX_CON_DB_SIZE);
        this.lastPrintUpdatePositionTimestamp = Instant.now().toEpochMilli();
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

    }

    public static JobHaDispatcherImpl getInstance(DbAgentJobManager jobManager, ProfileFetcher fetcher) {
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

    /**
     * task config info add to cache
     *
     * @param taskConf
     */
    @Override
    public void addJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("add job dbJobId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        this.updateJob(taskConf);
    }

    /**
     * start job statue
     *
     * @param taskConf conf
     */
    @Override
    public void startJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("start job dbJobId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        this.updateJob(taskConf);
    }

    /**
     * update job config info
     *
     * @param taskConf task conf
     */
    @Override
    public void updateJob(DbSyncTaskInfo taskConf) {
        LOGGER.info("update job dbJobId[{}], taskId[{}]", taskConf.getServerName(), taskConf.getId());
        // check haInfo is or not exist
        JobHaInfo jobHaInfo = jobHaInfoMap.computeIfAbsent(taskConf.getServerName(), (k) -> {
            JobHaInfo newJobHaInfo = new JobHaInfo();
            newJobHaInfo.setZkHealth(true);
            String jobInstanceName = taskConf.getDbServerInfo().getUrl() + ":" + taskConf.getServerName();
            newJobHaInfo.setJobName(jobInstanceName);
            newJobHaInfo.setDbJobId(k);
            return newJobHaInfo;
        });

        if (jobHaInfo != null) {
            // update config
            jobHaInfo.addTaskConf(taskConf);
        } else {
            LOGGER.error("Task conf is error taskId[{}], dbJobId[{}]", taskConf.getId(), taskConf.getServerName());
        }
    }

    /**
     * stop job statue
     *
     * @param dbJobId dbJobId
     */
    @Override
    public void stopJob(String dbJobId, Integer taskId, DbSyncTaskInfo taskConf) {
        LOGGER.info("stop job dbJobId[{}], taskId[{}]", dbJobId, taskId);
        deleteJob(dbJobId, taskId, taskConf);
    }

    /**
     * stop job
     *
     * @param jobHaInfo
     */
    private void stopJob(JobHaInfo jobHaInfo) {
        String jobName = jobHaInfo.getJobName();
        String dbJobId = jobHaInfo.getDbJobId();
        if (jobHaInfo.getTaskConfMap().size() > 0) {
            Set<Map.Entry<Integer, DbSyncTaskInfo>> entrySet =
                    jobHaInfo.getTaskConfMap().entrySet();
            for (Map.Entry<Integer, DbSyncTaskInfo> entry : entrySet) {
                stopOldRunningNodeTask(jobHaInfo, entry.getValue());
            }
        }
        try {
            if (StringUtils.isNotEmpty(jobName)) {
                this.jobManager.checkAndStopJobForce(jobName);
            }
            String position = jobSenderPosition.remove(dbJobId);
            if (position != null) {
                jobHaInfo.setSyncPosition(position);
                updatePositionToZk(jobHaInfo, jobHaInfo.getSyncPosition(), true);
            }
            jobHaInfo.getTaskConfMap().clear();
            LOGGER.info("Job dbJobId[{}] JobName = {} is stopped!", jobHaInfo.getDbJobId(), jobName);
        } catch (Throwable e) {
            LOGGER.error("checkAndStopJobForce has error e = {}", e);
        }
    }

    /**
     * delete job config info
     *
     * @param dbJobId dbJobId
     */
    @Override
    public void deleteJob(String dbJobId, Integer taskId, DbSyncTaskInfo taskConfFromTdm) {
        LOGGER.info("delete job dbJobId[{}], taskId[{}]", dbJobId, taskId);
        JobHaInfo jobHaInfo = jobHaInfoMap.get(dbJobId);
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
                        LOGGER.info("stop task has completed! dbJobId[{}], taskId[{}]", dbJobId, taskId);
                        synchronized (correctTaskInfoList) {
                            correctTaskInfoList.add(taskConf);
                        }
                    }
                });
            } else {
                // not exist, delete ok!
                LOGGER.info("cannot find local job, delete conf {}", taskConfFromTdm.getId());
                correctTaskInfoList.add(taskConfFromTdm);
            }
            // delete job when there is no any task
            synchronized (jobHaInfo) {
                if (jobHaInfo.getTaskConfMap().size() <= 0) {
                    jobHaInfoMap.remove(dbJobId);
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
                LOGGER.info("startAllJobOnce change task dbJobId[{}] , taskId[{}]",
                        conf.getServerName(), conf.getId());
            }
            Set<Map.Entry<String, List<DbSyncTaskInfo>>> entrySet = jobGroupMap.entrySet();
            for (Map.Entry<String, List<DbSyncTaskInfo>> entry : entrySet) {
                JobHaInfo value = jobHaInfoMap.computeIfPresent(entry.getKey(), (k, jobHaInfo) -> {
                    if (jobHaInfo != null) {
                        List<DbSyncTaskInfo> confList = entry.getValue();
                        LOGGER.info("startJobWithAllTaskOnce dbJobId[{}], taskSize = {}", jobHaInfo.getDbJobId(),
                                (confList == null ? 0 : confList.size()));
                        if (!checkValidConfig(entry, jobHaInfo, confList)) {
                            return jobHaInfo;
                        }
                        String startPositionFromZk = getStartPositionFromZk(jobHaInfo);
                        for (DbSyncTaskInfo taskConf : confList) {
                            LOGGER.info("startJobWithAllTaskOnce add task dbJobId[{}] "
                                    + ", taskId[{}]",
                                    taskConf.getServerName(), taskConf.getId());
                            CompletableFuture<Void> opFuture = new CompletableFuture<>();
                            initStartPosition(startPositionFromZk, taskConf);
                            jobHaInfo.setSyncPosition(taskConf.getStartPosition());
                            DBSyncJob dbJob = jobManager.addDbSyncTask(taskConf, opFuture);
                            if (dbJob != null) {
                                dbJobsMap.putIfAbsent(jobHaInfo.getDbJobId(), dbJob);
                            }
                            LOGGER.info("startJobWithAllTaskOnce add task dbJobId[{}] "
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
                                    LOGGER.info("update task has completed! dbJobId[{}], taskId[{}]",
                                            jobHaInfo.getDbJobId(), taskConf.getId());
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
             * when the running node under dbJobId is removed the jobInfo should also be removed from zk
             */
            if (taskConf.getNodeIps() == null || taskConf.getNodeIps().stream().noneMatch(a -> a.equals(localIp))) {
                LOGGER.info("dbJobId {}, task Conf {}, doesn't contains localIp {}, so remove "
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
     * @param dbJobId dbJobId
     * @param position postition
     */
    @Override
    public void updatePosition(String dbJobId, String position) {
        if (dbJobId == null) {
            LOGGER.error("[{}] dbJobId is null!", dbJobId);
            return;
        }
        jobSenderPosition.put(dbJobId, position);
        JobHaInfo jobHaInfo = jobHaInfoMap.get(dbJobId);
        if (jobHaInfo != null) {
            jobHaInfo.setSyncPosition(position);
        } else {
            LOGGER.error("[{}] running job is not exist in HaInfo", dbJobId);
        }
    }

    public boolean checkNodeRegisterStatus(Integer clusterId, String zkServer) {
        if (clusterId == null || StringUtils.isEmpty(zkServer)) {
            LOGGER.error("clusterId = {}, zkServer = {} has error!", clusterId, zkServer);
            return false;
        }
        ConfigDelegate configDelegate = getZkConfigDelegate(zkServer);
        if (configDelegate != null) {
            String jobCandidateParentPath =
                    ZkUtil.getCandidateParentPath(String.valueOf(clusterId));
            String checkPath = ZkUtil.getZkPath(jobCandidateParentPath, registerKey);
            boolean hasExist = configDelegate.checkPathIsExist(ConfigDelegate.ZK_GROUP, checkPath);
            if (hasExist) {
                LOGGER.error(
                        "Same registerKey app has started, please change localIp or agent uniq config to start again!"
                                + ",clusterId = {}, zkServer = {}, registerKey = {}!",
                        clusterId, zkServer, registerKey);
            }
            try {
                configDelegate.close();
            } catch (Exception e) {
                // LOGGER.error("Close configDelegate has error !", e);
            }
            return !hasExist;
        }
        return false;
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
                ZkUtil.getJobPositionPath(String.valueOf(clusterId), jobHaInfo.getDbJobId()));
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
            LOGGER.error("stop task has error {} dbJobId[{}], taskId[{}]", e,
                    jobHaInfo.getDbJobId(), taskConf.getId());
        }
        try {
            opFuture.get(1, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOGGER.error("stop task has TimeoutException e = {}, dbJobId[{}], taskId[{}]", e,
                    jobHaInfo.getDbJobId(), taskConf.getId());
        } catch (InterruptedException e) {
            LOGGER.error("stop task has InterruptedException e = {}, dbJobId[{}], taskId[{}]", e,
                    jobHaInfo.getDbJobId(), taskConf.getId());
        } catch (ExecutionException e) {
            LOGGER.error("stop task has ExecutionException e = {}, dbJobId[{}], taskId[{}]", e,
                    jobHaInfo.getDbJobId(), taskConf.getId());
        }
    }

    /**
     * update zk stats
     *
     * @param clusterId clusterId
     * @param dbJobId dbJobId
     * @param isConnected isConnected
     */
    @Override
    public void updateZkStats(String clusterId, String dbJobId, boolean isConnected) {
        if (StringUtils.isNotEmpty(dbJobId)) {
            JobHaInfo jobHaInfo = jobHaInfoMap.get(dbJobId);
            if (jobHaInfo != null && !jobHaInfo.isZkHealth() && isConnected) {
                initSyncInfoToZk(zkUrl, jobHaInfo.getDbJobId(), clusterId);
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
        running = false;
        if (jobStateMonitorExecutor != null && !jobStateMonitorExecutor.isShutdown()) {
            jobStateMonitorExecutor.shutdown();
        }

        if (positionUpdateExecutor != null && !positionUpdateExecutor.isShutdown()) {
            positionUpdateExecutor.shutdown();
        }

        loadBalanceService.close();

        if (jobCoordinator != null) {
            this.jobCoordinator.stop();
        }
    }

    private void updatePositionToZk(JobHaInfo jobHaInfo, String position, boolean printEnable) {
        if (StringUtils.isEmpty(position)) {
            return;
        }
        try {
            ConfigDelegate configDelegate = getZkConfigDelegate();
            configDelegate.createPathAndSetData(ConfigDelegate.ZK_GROUP,
                    ZkUtil.getJobPositionPath(String.valueOf(clusterId),
                            jobHaInfo.getDbJobId()),
                    position);
            if (printEnable) {
                MonitorLogUtils.printZkPositionMetric(jobHaInfo.getJobName(), position);
            }

        } catch (Throwable e) {
            LOGGER.error("updatePositionToZk has exception ", e);
        }
    }

    /**
     * ha dispatch node to stop run job
     *
     * @param dbJobId dbJobId
     */
    @Override
    public void removeLocalRunJob(String dbJobId) {
        if (needToRunDbJobIdSet.remove(dbJobId)) {
            LOGGER.info("remove dbJobId [{}] form local set!", dbJobId);
        }
    }

    /**
     * ha dispatch node to run job
     *
     * @param  dbJobId dbJobId
     * @param jobRunNodeInfo job run node info
     */
    @Override
    public void updateRunJobInfo(String dbJobId, JobRunNodeInfo jobRunNodeInfo) {
        if (jobRunNodeInfo != null && registerKey.equals(jobRunNodeInfo.getIp())) {
            if (jobRunNodeInfo.getRunningModel() != null
                    && jobRunNodeInfo.getRunningModel() == RUNNING_MODEL_STOPPED) {
                if (needToRunDbJobIdSet.remove(dbJobId)) {
                    LOGGER.info("remove dbJobId [{}] form local set by running model!", dbJobId);
                }
                return;
            } else if (needToRunDbJobIdSet.add(dbJobId)) {
                LOGGER.info("add dbJobId [{}] to local set!", dbJobId);
            }
        } else {
            if (needToRunDbJobIdSet.remove(dbJobId)) {
                LOGGER.info("remove dbJobId [{}] form local set!", dbJobId);
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
    public void updateJobCoordinator(String clusterId, String changeCoordinatorPath, boolean isAdd) {
        if (clusterId == null || !running) {
            LOGGER.warn("clusterId {} is null or is not running {}", clusterId, running);
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
            byte[] bytes = configDelegate.getData(ConfigDelegate.ZK_GROUP, jobCoordinatorParentPath
                    + "/" + paths.get(0));
            if (bytes != null) {
                jobCoordinatorIp = new String(bytes);
            } else {
                jobCoordinatorIp = "";
            }
            currentCoordinatorNode = paths.get(0);
        } else {
            this.isJobCoordinator = false;
        }

        LOGGER.info("updateJobCoordinator isAdd [{}] changeCoordinatorPath [{}], localRegisterCoordinatorPath [{}]",
                isAdd, changeCoordinatorPath, localRegisterCoordinatorPath);

        if (!isAdd && StringUtils.isNotEmpty(changeCoordinatorPath)
                && (changeCoordinatorPath.equals(localRegisterCoordinatorPath))) {
            setNeedRetryRegisterLocalCoordinator(true);
        }

        LOGGER.info("updateJobCoordinator isJobCoordinator [{}], currentCoordinatorNode [{}]",
                isJobCoordinator, currentCoordinatorNode);
    }

    private synchronized void setNeedRetryRegisterLocalCoordinator(boolean value) {
        isNeedRetryRegisterLocalCoordinator = value;
    }

    @Override
    public boolean updateDbJobIdList(Integer newClusterId, Integer dbJobIdListVersion,
            String zkServer, List<String> dbJobIdList,
            boolean needCheckClusterIdOrDbJobIdListVersion) {
        if (newClusterId == null || StringUtils.isEmpty(zkServer)) {
            LOGGER.error("newClusterId = {}, zkServer = {} has error!", newClusterId, zkServer);
            return false;
        }
        LOGGER.info("newClusterId = {}, clusterId = {}, zkServer = {}!", newClusterId, clusterId, zkServer);
        if ((!newClusterId.equals(clusterId))
                || (zkUrl == null || !zkUrl.equals(zkServer))) {
            if (checkNodeRegisterStatus(newClusterId, zkServer)) {
                LOGGER.info("change zkUrl Info from old = {} , to new = {}", zkUrl, zkServer);
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
                    this.configDelegate = null;
                }
                zkUrl = zkServer;
                initClusterInfoToZk(newClusterId);
                loadBalanceService.setLocalCandidatePath(String.valueOf(newClusterId), registerKey);
                loadBalanceService.setClusterId(String.valueOf(newClusterId));
            } else {
                return false;
            }
        }

        if (!newClusterId.equals(clusterId) || this.dbJobIdListVersion != dbJobIdListVersion
                || !needCheckClusterIdOrDbJobIdListVersion) {
            Set<String> currentDbJobIdList = this.allDbJobIdClusterSet;
            /*
             * clean old
             */
            List cleanDbJobIdList = null;
            for (String dbJobId : currentDbJobIdList) {
                if (!dbJobIdList.contains(dbJobId)) {
                    if (cleanDbJobIdList == null) {
                        cleanDbJobIdList = new ArrayList<>();
                    }
                    cleanDbJobIdList.add(dbJobId);
                }
            }
            if (cleanDbJobIdList != null) {
                this.allDbJobIdClusterSet.removeAll(cleanDbJobIdList);
                cleanMovedJobInfo(cleanDbJobIdList, zkServer);
            }
            LOGGER.info("update clusterId {}/{} or dbJobIdListVersion {}/{}!", clusterId,
                    newClusterId, this.dbJobIdListVersion, dbJobIdListVersion);
            /*
             * register new info to zk
             */
            if (dbJobIdList != null && dbJobIdList.size() > 0) {
                for (String dbJobId : dbJobIdList) {
                    if (!this.allDbJobIdClusterSet.contains(dbJobId)) {
                        initSyncInfoToZk(zkServer, dbJobId, String.valueOf(newClusterId));
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("allDbJobIdClusterSet add dbJobId {}!", dbJobId);
                        }

                        this.allDbJobIdClusterSet.add(dbJobId);
                    }
                }
            }
            this.clusterId = newClusterId;
            this.dbJobIdListVersion = dbJobIdListVersion;
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
        if (newClusterId != null && !newClusterId.equals(clusterId)) {

            if (StringUtils.isEmpty(zkUrl) || newClusterId == null) {
                LOGGER.warn("zkServer = {}, clusterId = {} params has error", zkUrl,
                        newClusterId);
                return false;
            }
            LOGGER.info("initClusterInfoToZk zkServer = {}, newClusterId = {}, clusterId = {}",
                    zkUrl, newClusterId, clusterId);
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
            registerForCoordinator(coordinatorNodeParentPath);

            /*
             * create candidate path and register
             */
            String jobCandidateParentPath =
                    ZkUtil.getCandidateParentPath(String.valueOf(newClusterId));
            LoadBalanceInfo initLoadBalanceInfo = new LoadBalanceInfo();
            initLoadBalanceInfo.setIp(registerKey);
            initLoadBalanceInfo.setMaxDbJobsThreshold(maxDbJobsThreshold);
            configDelegate.createEphemeralPathAndSetData(ConfigDelegate.ZK_GROUP,
                    ZkUtil.getZkPath(jobCandidateParentPath, registerKey),
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

    private boolean registerForCoordinator(String coordinatorNodeParentPath) {
        String coordinatorPath =
                configDelegate.createOrderEphemeralPathAndSetData(ConfigDelegate.ZK_GROUP,
                        ZkUtil.getZkPath(coordinatorNodeParentPath, Constants.ZK_FOR_LEADER), localIp);
        if (StringUtils.isNotEmpty(coordinatorPath)) {
            localRegisterCoordinatorPath = ZkUtil.getLastNodePath(coordinatorPath);
            setNeedRetryRegisterLocalCoordinator(false);
            return true;
        }
        setNeedRetryRegisterLocalCoordinator(true);
        return false;
    }

    /**
     * init and regist dbJobId to zk
     *
     * @param zkServer
     * @param dbJobId
     * @return
     */
    private boolean initSyncInfoToZk(String zkServer, String dbJobId, String clusterId) {
        if (StringUtils.isEmpty(zkServer) || StringUtils.isEmpty(dbJobId)
                || StringUtils.isEmpty(clusterId)) {
            LOGGER.warn("zkServer = {}, dbJobId = {}, clusterId = {} params has error",
                    zkServer, dbJobId, clusterId);
            return false;
        }
        LOGGER.info("initInfoToZk zkServer = {}, dbJobId = {}", zkServer, dbJobId);
        ConfigDelegate configDelegate = getZkConfigDelegate();

        /*
         * 1 create job run node info
         */
        configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobRunNodePath(clusterId, dbJobId));

        /*
         * 2 create position node
         */
        configDelegate.createIfNeededPath(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobPositionPath(clusterId, dbJobId));

        String jobPath = ZkUtil.getJobRunNodePath(clusterId, dbJobId);
        configDelegate.removeNodeListener(ConfigDelegate.ZK_GROUP, jobPath);
        configDelegate.addNodeListener(jobRunNodeChangeListener, ConfigDelegate.ZK_GROUP, jobPath);

        return true;
    }

    private boolean cleanSyncInfoFromZk(String zkServer, String dbJobId) {
        if (StringUtils.isEmpty(zkServer) || StringUtils.isEmpty(dbJobId)) {
            LOGGER.warn("zkServer = {}, dbJobId = {} params has error", zkServer, dbJobId);
            return false;
        }
        LOGGER.info("cleanSyncInfoFromZk zkServer = {}, dbJobId = {}", zkServer, dbJobId);
        ConfigDelegate configDelegate = getZkConfigDelegate();
        configDelegate.removeNodeListener(ConfigDelegate.ZK_GROUP,
                ZkUtil.getJobRunNodePath(String.valueOf(clusterId), dbJobId));
        return false;
    }

    private void cleanMovedJobInfo(List<Integer> serverIdList, String zkUrl) {
        if (serverIdList != null) {
            for (Integer dbJobId : serverIdList) {
                JobHaInfo jobHaInfo = jobHaInfoMap.remove(String.valueOf(dbJobId));
                if (jobHaInfo != null) {
                    stopJob(jobHaInfo);
                }
                cleanSyncInfoFromZk(zkUrl, String.valueOf(dbJobId));
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
    public Integer getDbJobIdListVersion() {
        return dbJobIdListVersion;
    }

    @Override
    public Set<String> getNeedToRunDbJobIdSet() {
        return needToRunDbJobIdSet;
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
    public int getMaxDbJobsThreshold() {
        return maxDbJobsThreshold;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    @Override
    public String getJobCoordinatorIp() {
        return jobCoordinatorIp;
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

    /**
     * get zk configDelegate
     *
     * @return ConfigDelegateImpl
     */
    private ConfigDelegate getZkConfigDelegate(String zkUrl) {
        if (StringUtils.isEmpty(zkUrl)) {
            return null;
        }
        Map<String, String> connectStrMap = new HashMap<>();
        connectStrMap.put(ConfigDelegate.ZK_GROUP, zkUrl);
        return new ConfigDelegateImpl(connectStrMap);
    }

    private CompletableFuture addDbJobIdAsync(String dbJobId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                haFetcher.initTaskInfoByDbJobId(dbJobId, getClusterId(),
                        getDbJobIdListVersion());
            } catch (Exception e) {
                LOGGER.error("initTaskInfoByServerId dbJobId[{}] has exception", dbJobId, e);
            }
            return true;
        }).thenAccept(u -> {
            LOGGER.info("addServerNameAsync dbJobId[{}] finished", dbJobId);
        }).exceptionally(ex -> {
            LOGGER.error("addServerNameAsync dbJobId[{}] finished has exception", dbJobId, ex);
            return null;
        });
    }

    private CompletableFuture removeServerNameAsync(JobHaInfo jobHaInfo) {
        if (jobHaInfo == null) {
            return null;
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                stopJob(jobHaInfo);
            } catch (Exception e) {
                LOGGER.error("stop job by dbJobId[{}] has exception e = {}", jobHaInfo.getDbJobId(), e);
            }
            return true;
        }).thenAccept(u -> {
            needToStopJobHaInfo.remove(jobHaInfo);
            jobHaInfoMap.remove(jobHaInfo.getDbJobId());
            LOGGER.info("stop job dbJobId[{}] finished!", jobHaInfo.getDbJobId());
        }).exceptionally(ex -> {
            LOGGER.error("stop job dbJobId[{}] finished has exception e = {}", jobHaInfo.getDbJobId(), ex);
            return null;
        });
    }

    private boolean needPrint() {
        long now = Instant.now().toEpochMilli();
        if ((now - lastPrintUpdatePositionTimestamp) > printUpdatePositionInterval) {
            lastPrintUpdatePositionTimestamp = now;
            return true;
        }
        return false;
    }

    private Runnable getPositionUpdateTask() {
        return () -> {
            try {
                if (!running) {
                    LOGGER.warn("job ha dispatcher is closed, so do nothing of position update "
                            + "task!");
                    return;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("PositionUpdateTask is running! report size [{}]",
                            jobSenderPosition.size());
                }

                boolean isNeedPrint = needPrint();
                jobSenderPosition.forEach((k, v) -> {
                    JobHaInfo jobHaInfo = jobHaInfoMap.get(k);
                    String dbJobId = (jobHaInfo == null ? null : jobHaInfo.getDbJobId());
                    if (dbJobId != null && needToRunDbJobIdSet.contains(dbJobId)) {
                        updatePositionToZk(jobHaInfo, v, isNeedPrint);
                    } else {
                        LOGGER.warn("[{}] running job is not exist in needToRunServerNameSet", k);
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
                if (!running) {
                    LOGGER.warn("job ha dispatcher is closed, so do nothing of state monitor!");
                    return;
                }
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
                    jobCoordinator.updateServerIDList(currentClusterId, this.dbJobIdListVersion,
                            allDbJobIdClusterSet);
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
                 * old dbJobId in needToRunSyncIdSet but has been removed, and need to be stop!
                 */
                jobHaInfoMap.forEach((dbJobId, jobHaInfo) -> {
                    if (!needToRunDbJobIdSet.contains(dbJobId)) {
                        jobHaInfo = jobHaInfoMap.get(dbJobId);
                        if (jobHaInfo != null) {
                            needToStopJobHaInfo.add(jobHaInfo);
                        }
                    }
                });

                /*
                 * retry register local for coordinate
                 */
                if (isNeedRetryRegisterLocalCoordinator) {
                    try {
                        String coordinatorNodeParentPath =
                                ZkUtil.getCoordinatorParentPath(String.valueOf(clusterId));
                        registerForCoordinator(coordinatorNodeParentPath);
                    } catch (Exception e) {
                        LOGGER.error("retry to register local for coordinator clusterId = {} has "
                                + "exception!", clusterId, e);
                    }
                }

                /*
                 * start stop
                 */
                List<CompletableFuture> futures = needToStopJobHaInfo.stream().map((jobHaInfo) -> {
                    LOGGER.info("Monitor stop dbJobId[{}]!", jobHaInfo.getDbJobId());
                    CompletableFuture re = removeServerNameAsync(jobHaInfo);
                    LOGGER.info("Monitor old dbJobId[{}] is stopped!", jobHaInfo.getDbJobId());
                    return re;
                }).collect(Collectors.toList());
                CompletableFuture allOf =
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                allOf.join();

                /*
                 * new dbJobId in needToRunServerNameSet but not start run
                 */
                futures = needToRunDbJobIdSet.stream().map((dbJobId) -> {
                    JobHaInfo jobHaInfo = jobHaInfoMap.get(dbJobId);
                    if (!running) {
                        return null;
                    }
                    if (jobHaInfo == null || !jobManager.isRunningJob(dbJobId)) {
                        LOGGER.info("Monitor start new dbJobId[{}]", dbJobId);
                        CompletableFuture re = addDbJobIdAsync(dbJobId);
                        LOGGER.info("Monitor new dbJobId[{}] is started", dbJobId);
                        return re;
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Monitor dbJobId[{}] is running now!", dbJobId);
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