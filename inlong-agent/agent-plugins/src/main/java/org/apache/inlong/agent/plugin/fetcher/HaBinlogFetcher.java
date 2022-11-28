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

package org.apache.inlong.agent.plugin.fetcher;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.ha.JobHaDispatcher;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.core.job.JobConfManager;
import org.apache.inlong.agent.core.job.JobManager;
import org.apache.inlong.agent.entites.CommonResponse;
import org.apache.inlong.agent.except.DataSourceConfigException.InvalidCharsetNameException;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.common.enums.ManagerOpEnum;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncClusterInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncInitInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskFullInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.inlong.common.pojo.agent.dbsync.InitTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest.TaskInfoBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_CONN_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_CONN_INTERVAL;
import static org.apache.inlong.agent.constant.FetcherConstants.DBSYNC_GET_SERVER_LIST;
import static org.apache.inlong.agent.constant.FetcherConstants.DBSYNC_REPORT_AND_GET_TASK;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_DBSYNC_GET_SERVER_LIST;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_DBSYNC_REPORT_AND_GET_TASK;
import static org.apache.inlong.agent.plugin.fetcher.ManagerResultFormatter.checkTdmReturnMsg;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalIp;

public class HaBinlogFetcher extends AbstractDaemon implements ProfileFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaBinlogFetcher.class);

    private static volatile boolean bInit = false; // first start flag
    private final AgentManager agentManager;
    private final JobManager jobManager;
    private final JobConfManager jobConfManager;
    private final HttpManager httpManager;
    private String localIp;
    private JobHaDispatcher jobHaDispatcher;
    private AgentConfiguration conf; //
    private String getServerIdListByIpUrl;
    private String reportTaskConfigPeriodUrl;
    private ArrayList<DBSyncJobConf> newJobs;
    private ConcurrentHashMap<Integer, TaskInfoBean> taskRegisterResults;
    private String clusterTag;
    private String clusterName;

    public HaBinlogFetcher(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.jobManager = agentManager.getJobManager();
        this.conf = AgentConfiguration.getAgentConf();
        this.localIp = fetchLocalIp();
        this.clusterName = conf.get(AGENT_CLUSTER_NAME);
        this.clusterTag = conf.get(AGENT_CLUSTER_TAG);
        this.httpManager = new HttpManager(conf);
        this.getServerIdListByIpUrl = buildGetInitInfoUrl();
        this.reportTaskConfigPeriodUrl = buildReportAndGetTaskUrl();
        this.taskRegisterResults = new ConcurrentHashMap<>();
        this.newJobs = new ArrayList<>();
        this.jobConfManager = new JobConfManager();
        this.jobHaDispatcher = JobHaDispatcherImpl.getInstance(jobManager, this);
    }

    private String buildGetInitInfoUrl() {
        return HttpManager.buildBaseUrl() + conf.get(DBSYNC_GET_SERVER_LIST, DEFAULT_DBSYNC_GET_SERVER_LIST);
    }

    private String buildReportAndGetTaskUrl() {
        return HttpManager.buildBaseUrl() + conf.get(DBSYNC_REPORT_AND_GET_TASK, DEFAULT_DBSYNC_REPORT_AND_GET_TASK);
    }

    private Runnable binlogHaFetchThread() {
        return () -> {
            LOGGER.info("HaBinlogFetcher Communicator Thread running!");
            long connInterval = conf.getLong(DBSYNC_CONN_INTERVAL, DEFAULT_DBSYNC_CONN_INTERVAL);
            long interval = 5 * connInterval;
            Random random = new Random();
            while (isRunnable()) {
                if (bInit) {
                    interval = connInterval / 2 + ((long) (random.nextFloat() * connInterval / 2));
                }
                try {
                    if (!bInit) {
                        if (initSyncIdListInfoByIp()) {
                            bInit = true;
                        }
                    } else {
                        reportAndGetTaskConfigPeriod();
                    }
                } catch (Throwable t) {
                    LOGGER.error("HaBinlogFetcher getException : {}", DBSyncUtils.getExceptionStack(t));
                }
                DBSyncUtils.sleep(interval);
            }
        };
    }

    @Override
    public void start() throws Exception {
        submitWorker(binlogHaFetchThread());
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    @Override
    public List<JobProfile> getJobProfiles() {
        return null;
    }

    @Override
    public List<TriggerProfile> getTriggerProfiles() {
        return null;
    }

    /**
     * get server id list info by ip when first start
     */
    public boolean initSyncIdListInfoByIp() throws Exception {
        int syncIdListSize = -1;

        InitTaskRequest initTaskRequest = new InitTaskRequest(clusterTag, clusterName, localIp);
        String syncIdListInfo = httpManager.doSentPost(getServerIdListByIpUrl, initTaskRequest);
        CommonResponse<DbSyncInitInfo> commonResponse =
                CommonResponse.fromJson(syncIdListInfo, DbSyncInitInfo.class);
        if (commonResponse == null || !parseSyncInfoAndRegisterInfo(commonResponse)) {
            throw new Exception("get task's configs has error by local Ip!");
        }
        if (commonResponse.getData() != null) {
            List<String> syncIdList = commonResponse.getData().getServerNames();
            if (syncIdList != null) {
                syncIdListSize = syncIdList.size();
            }
        }
        LOGGER.info("init get server url [{}], syncIdListSize size = {}",
                getServerIdListByIpUrl, syncIdListSize);
        return true;
    }

    public boolean reportAndGetTaskConfigPeriod() throws Exception {
        ReportTaskRequest reportTaskRequest = new ReportTaskRequest();
        reportTaskRequest.setIp(localIp);
        reportTaskRequest.setClusterTag(clusterTag);
        reportTaskRequest.setClusterName(clusterName);

        if (jobHaDispatcher.getClusterId() != -1) {
            DbSyncClusterInfo clusterInfo = new DbSyncClusterInfo();
            clusterInfo.setParentId(jobHaDispatcher.getClusterId());
            clusterInfo.setClusterName(clusterName);
            clusterInfo.setServerVersion(jobHaDispatcher.getSyncIdListVersion());
            reportTaskRequest.setDbSyncCluster(clusterInfo);
        }

        // handle error task config
        initTaskRegisterResults();
        reportTaskRequest.setTaskInfoList(new ArrayList<>(taskRegisterResults.values()));

        reportTaskRequest.setServerNames(jobManager.getCurrentRunSyncIdList());

        String jobConfigString = httpManager.doSentPost(reportTaskConfigPeriodUrl, reportTaskRequest);
        CommonResponse<DbSyncTaskFullInfo> commonResponse =
                CommonResponse.fromJson(jobConfigString, DbSyncTaskFullInfo.class);
        parseJobAndCheckForStart(commonResponse);
        return true;
    }

    private boolean parseSyncInfoAndRegisterInfo(CommonResponse<DbSyncInitInfo> commonResponse) {
        boolean result = false;
        if (commonResponse != null) {
            if (checkTdmReturnMsg(commonResponse)) {
                DbSyncInitInfo syncIdListInfo = commonResponse.getData();
                if (syncIdListInfo != null && StringUtils.isNotEmpty(syncIdListInfo.getZkUrl())) {
                    DbSyncClusterInfo clusterInfo = syncIdListInfo.getCluster();
                    Integer parentId = clusterInfo.getParentId();
                    Integer syncIdListVersion = clusterInfo.getServerVersion();
                    List<String> syncInfoList = syncIdListInfo.getServerNames();
                    result = handlerSyncIdListVersionChange(parentId, syncIdListVersion, syncIdListInfo.getZkUrl(),
                            syncInfoList);
                } else {
                    LOGGER.error("serverIdListInfo or zkUrl is empty!");
                }
            }
        }
        return result;
    }

    private void initTaskRegisterResults() {
        List<DbSyncTaskInfo> errorTaskConfigList = jobHaDispatcher.getErrorTaskConfInfList();
        if (errorTaskConfigList != null && errorTaskConfigList.size() > 0) {
            for (DbSyncTaskInfo tc : errorTaskConfigList) {
                taskRegisterResults.put(tc.getId(),
                        new TaskInfoBean(tc.getId(), tc.getStatus(),
                                "ha addTask has task conf info error! ip:" + localIp, 1, tc.getVersion()));
                LOGGER.error("Error task config syncId {}, taskId {}", tc.getServerName(), tc.getId());
            }
        }
        List<DbSyncTaskInfo> correctTaskConfigList = jobHaDispatcher.getCorrectTaskConfInfList();
        if (correctTaskConfigList != null && correctTaskConfigList.size() > 0) {
            for (DbSyncTaskInfo tc : correctTaskConfigList) {
                taskRegisterResults.put(tc.getId(),
                        new TaskInfoBean(tc.getId(), tc.getStatus(), "config success! ip: " + localIp, 0,
                                tc.getVersion()));
                LOGGER.info("task config success syncId {}, taskId {}!", tc.getServerName(), tc.getId());
            }
        }
        List<DbSyncTaskInfo> exceedTaskConfigList = jobHaDispatcher.getExceedTaskInfList();
        if (exceedTaskConfigList != null && exceedTaskConfigList.size() > 0) {
            for (DbSyncTaskInfo tc : exceedTaskConfigList) {
                taskRegisterResults.put(tc.getId(),
                        new TaskInfoBean(tc.getId(), tc.getStatus(), "exceed job max limit, ip: " + localIp, -1,
                                tc.getVersion()));
                LOGGER.error("task config error syncId {}, taskId{}! exceed max job", tc.getServerName(), tc.getId());
            }
        }
    }

    /**
     * handler severIdListVersion or clusterId change
     *
     * @param newClusterId
     * @param newSyncIdListVersion
     * @param zkUrl
     * @param syncIdList
     */
    private boolean handlerSyncIdListVersionChange(Integer newClusterId,
            Integer newSyncIdListVersion,
            String zkUrl,
            List<String> syncIdList) {
        return jobHaDispatcher.updateSyncIdList(newClusterId, newSyncIdListVersion, zkUrl,
                syncIdList, true);
    }

    @Override
    public boolean parseJobAndCheckForStart(CommonResponse<DbSyncTaskFullInfo> commonResponse) {
        boolean result = false;
        if (checkTdmReturnMsg(commonResponse)) {
            taskRegisterResults.clear();
            DbSyncTaskFullInfo dbSyncTaskFullInfo = commonResponse.getData();
            if (dbSyncTaskFullInfo != null && StringUtils.isNotEmpty(dbSyncTaskFullInfo.getZkUrl())) {
                DbSyncClusterInfo clusterInfo = dbSyncTaskFullInfo.getCluster();
                String zkUrl = dbSyncTaskFullInfo.getZkUrl();
                if (clusterInfo != null) {
                    List<String> syncIdsList = dbSyncTaskFullInfo.getChangedServers();
                    List<String> offlineServerIdList = dbSyncTaskFullInfo.getOfflineServers();
                    if (syncIdsList != null && syncIdsList.size() > 0) {
                        result = handlerSyncIdListVersionChange(clusterInfo.getParentId(),
                                clusterInfo.getServerVersion(), zkUrl, syncIdsList);
                        if (!result) {
                            return false;
                        }
                    }
                    if (offlineServerIdList != null && offlineServerIdList.size() > 0) {
                        result = handlerForceRemoveServerIds(clusterInfo.getParentId(),
                                clusterInfo.getServerVersion(), zkUrl, offlineServerIdList);
                        if (!result) {
                            return false;
                        }
                    }
                    List<DbSyncTaskInfo> taskConfList = dbSyncTaskFullInfo.getTaskInfoList();
                    if (taskConfList != null && taskConfList.size() > 0) {
                        List<DBSyncJob> newJobs = parseJob(taskConfList);
                        if (newJobs != null) {
                            jobManager.startAllInitJobs(newJobs);
                        }
                    }
                    result = true;
                } else {
                    LOGGER.error("clusterInfo is null!");
                }
            } else {
                LOGGER.error("DbSyncTaskFullInfo or zkUrl is empty!");
            }
        }
        return result;
    }

    /**
     * parse information from tdmamanger construct new job conf or update
     * exist job configure
     */
    private List<DBSyncJob> parseJob(List<DbSyncTaskInfo> taskConfList) {
        LOGGER.debug("Array: {}, size :{}", taskConfList, taskConfList.size());
        Iterator<DbSyncTaskInfo> iterator = taskConfList.iterator();
        boolean isHaEnable = true; // TODO: remove
        List<DbSyncTaskInfo> changeTaskList = new ArrayList<>();
        Map<String, DBSyncJob> dbJobsMap = new HashMap<>();
        while (iterator.hasNext()) {
            DBSyncJob dbJob = null;
            DbSyncTaskInfo taskConf = iterator.next();
            LOGGER.warn("Object: {}, status :{}", taskConf, taskConf.getStatus());

            Integer status = taskConf.getStatus();
            Integer taskId = taskConf.getId();
            final Integer version = taskConf.getVersion();
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                // 0 add; 4 stop; 5 run; 1 del; 2 modify
                if (status == ManagerOpEnum.ADD.getType()) {
                    if (isHaEnable && checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.addJob(taskConf);
                        changeTaskList.add(taskConf);
                    } else {
                        dbJob = jobManager.addDbSyncTask(taskConf, future);
                    }
                } else if (status == ManagerOpEnum.FROZEN.getType()) {
                    if (isHaEnable) {
                        jobHaDispatcher.stopJob(taskConf.getServerName(), taskConf.getId());
                        future.complete(null);
                    } else {
                        deleteTask(taskConf, future);
                    }
                } else if (status == ManagerOpEnum.ACTIVE.getType()) {
                    if (isHaEnable && checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.startJob(taskConf);
                        changeTaskList.add(taskConf);
                    } else {
                        dbJob = jobManager.addDbSyncTask(taskConf, future);
                    }
                } else if (status == ManagerOpEnum.DEL.getType()) {
                    if (isHaEnable) {
                        jobHaDispatcher.deleteJob(taskConf.getServerName(), taskConf.getId(), taskConf);
                        future.complete(null);
                    } else {
                        stopTask(taskConf, future);
                    }
                } else if (status == ManagerOpEnum.RETRY.getType()) {
                    if (isHaEnable && checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.updateJob(taskConf);
                        changeTaskList.add(taskConf);
                    } else {
                        dbJob = jobManager.addDbSyncTask(taskConf, future);
                    }
                } else if (isHaEnable && status == 101) {
                    if (checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.updateJob(taskConf);
                        changeTaskList.add(taskConf);
                    }
                } else {
                    LOGGER.error("Get task config in error status : {}", taskConf);
                }

                if (dbJob != null) {
                    dbJobsMap.putIfAbsent(taskConf.getServerName(), dbJob);
                }

                if (!isHaEnable) {
                    future.thenAccept(ignore -> taskRegisterResults.put(taskId,
                            new TaskInfoBean(taskId, status, "config success", 0, version)))
                            .exceptionally(t -> {
                                String msg = t.getCause().getMessage();
                                taskRegisterResults.put(taskId, new TaskInfoBean(taskId, status, msg, 1, version));
                                LOGGER.error("Op Job {} exception: {}", taskId, msg);
                                return null;
                            });
                }
            } catch (Exception e) {
                LOGGER.error("op-{} task {} getException", status, taskConf.getId(), e);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOGGER.error("sleep interrupted", e);
            }
        }
        if (isHaEnable) {
            return jobHaDispatcher.startAllTasksOnce(changeTaskList);
        }
        return dbJobsMap.values().stream().collect(Collectors.toList());
    }

    /**
     * now, I think stop an task is the same to delete an task
     *
     * @param taskConf
     */
    private void stopTask(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        deleteTask(taskConf, future);
    }

    /**
     * delelte the task from job, if job not db data need dump, stop the job
     *
     * @param taskConf
     */
    public synchronized void deleteTask(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        Integer taskId = taskConf.getId();
        // String ip = taskConf.getIps();
        String dbName = taskConf.getDbName();
        String tableName = taskConf.getTableName();

        LOGGER.debug("Get taskId :{}, dbName:{}, tbName:{} to delete task!",
                taskId, dbName, tableName);

        DBSyncJobConf conf = jobConfManager.getConfByTaskId(taskId);
        if (conf == null) {
            LOGGER.warn("can't find task_id {} ", taskId);
            // not exist, delete ok!
            future.complete(null);
        } else {
            String jobName = conf.getJobName();
            conf.removeTable(taskId);
            if (conf.bNoNeedDb()) {
                jobManager.stopJob(jobName);
            }
            future.complete(null);
        }
    }

    private boolean checkTaskConfig(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        Charset charset = null;
        try {
            if (org.apache.commons.lang.StringUtils.isNotBlank(taskConf.getCharset())) {
                charset = Charset.forName(taskConf.getCharset());
            } else {
                charset = StandardCharsets.UTF_8;
            }
        } catch (Exception e) {
            LOGGER.error("invalid charset name: {}, {}", taskConf.getCharset(), e.getMessage());
            future.completeExceptionally(new InvalidCharsetNameException(
                    "invalid charset name " + taskConf.getCharset()));
            return false;
        }
        future.complete(null);
        return true;
    }

    /**
     * handle serverId delete
     *
     * @param newClusterId
     * @param newServerIdListVersion
     * @param zkUrl
     * @param serverIdList
     */
    private boolean handlerForceRemoveServerIds(Integer newClusterId,
            Integer newServerIdListVersion,
            String zkUrl,
            List<String> serverIdList) {
        return jobHaDispatcher.updateSyncIdList(newClusterId, newServerIdListVersion, zkUrl,
                serverIdList, false);
    }
}
