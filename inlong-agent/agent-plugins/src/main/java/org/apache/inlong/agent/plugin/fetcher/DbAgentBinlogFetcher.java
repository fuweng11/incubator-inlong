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

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.core.DbAgentManager;
import org.apache.inlong.agent.core.dbsync.DBSyncJob;
import org.apache.inlong.agent.core.dbsync.DbAgentJobManager;
import org.apache.inlong.agent.core.dbsync.IDbAgentProfileFetcher;
import org.apache.inlong.agent.core.dbsync.ha.JobHaDispatcher;
import org.apache.inlong.agent.core.dbsync.ha.JobHaDispatcherImpl;
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
import org.apache.inlong.common.pojo.agent.dbsync.RunningTaskRequest;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.agent.constant.AgentConstants.*;
import static org.apache.inlong.agent.constant.FetcherConstants.*;
import static org.apache.inlong.agent.plugin.fetcher.ManagerResultFormatter.checkTdmReturnMsg;
import static org.apache.inlong.agent.utils.AgentUtils.fetchLocalIp;

public class DbAgentBinlogFetcher extends AbstractDaemon implements ProfileFetcher, IDbAgentProfileFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbAgentBinlogFetcher.class);

    private static volatile boolean bInit = false; // first start flag
    private final DbAgentJobManager jobManager;
    private final HttpManager httpManager;
    private String localIp;
    private JobHaDispatcher jobHaDispatcher;
    private AgentConfiguration agentConf; //
    private String getServerNameListByIpUrl;
    private String reportTaskConfigPeriodUrl;
    private String getTaskConfigByIpAndServerIdUrl;
    private ConcurrentHashMap<Integer, TaskInfoBean> taskRegisterResults;
    private final String clusterTag;
    private final String clusterName;
    private final String agentUniq;

    public DbAgentBinlogFetcher(DbAgentManager agentManager) {
        this.jobManager = agentManager.getJobManager();
        this.agentConf = AgentConfiguration.getAgentConf();
        this.localIp = fetchLocalIp();
        this.clusterName = agentConf.get(AGENT_CLUSTER_NAME);
        this.clusterTag = agentConf.get(AGENT_CLUSTER_TAG);
        this.agentUniq = agentConf.get(AGENT_UNIQ_ID, "1");
        this.httpManager = new HttpManager(agentConf);
        this.getServerNameListByIpUrl = buildGetInitInfoUrl();
        this.reportTaskConfigPeriodUrl = buildReportAndGetTaskUrl();
        this.getTaskConfigByIpAndServerIdUrl = buildGetRunningTaskUrl();
        this.taskRegisterResults = new ConcurrentHashMap<>();
        this.jobHaDispatcher = JobHaDispatcherImpl.getInstance(jobManager, this);
    }

    private String buildGetInitInfoUrl() {
        return HttpManager.buildBaseUrl() + agentConf.get(DBSYNC_GET_SERVER_LIST, DEFAULT_DBSYNC_GET_SERVER_LIST);
    }

    private String buildReportAndGetTaskUrl() {
        return HttpManager.buildBaseUrl()
                + agentConf.get(DBSYNC_REPORT_AND_GET_TASK, DEFAULT_DBSYNC_REPORT_AND_GET_TASK);
    }

    private String buildGetRunningTaskUrl() {
        return HttpManager.buildBaseUrl() + agentConf.get(DBSYNC_GET_RUNNING_TASKS, DEFAULT_DBSYNC_GET_RUNNING_TASKS);
    }

    private Runnable binlogHaFetchThread() {
        return () -> {
            LOGGER.info("HaBinlogFetcher Communicator Thread running!");
            long connInterval = agentConf.getLong(DBSYNC_CONN_INTERVAL, DEFAULT_DBSYNC_CONN_INTERVAL);
            while (isRunnable()) {
                try {
                    if (!bInit) {
                        if (initDbJobIdListByIp()) {
                            bInit = true;
                        }
                    } else {
                        reportAndGetTaskConfigPeriod();
                    }
                } catch (Throwable t) {
                    LOGGER.error("HaBinlogFetcher getException : {}", DBSyncUtils.getExceptionStack(t));
                }
                DBSyncUtils.sleep(connInterval);
            }
        };
    }

    @Override
    public void start() throws Exception {
        submitWorker(binlogHaFetchThread());
    }

    @Override
    public void stop() throws Exception {
        jobHaDispatcher.close();
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
    public boolean initDbJobIdListByIp() throws Exception {
        int dbJobIdListSize = -1;

        InitTaskRequest initTaskRequest = new InitTaskRequest(clusterTag, clusterName, localIp);
        String dbJobIdListInfo = httpManager.doSentPost(getServerNameListByIpUrl, initTaskRequest);
        CommonResponse<DbSyncInitInfo> commonResponse =
                CommonResponse.fromJson(dbJobIdListInfo, DbSyncInitInfo.class);
        if (commonResponse == null || !parseSyncInfoAndRegisterInfo(commonResponse)) {
            String errorMsg = "Get task's configs has error by local Ip [" + localIp
                    + " ] or some local config has error!";
            throw new Exception(errorMsg);
        }
        if (commonResponse.getData() != null) {
            List<String> dbJobIdList = commonResponse.getData().getServerNames();
            if (dbJobIdList != null) {
                dbJobIdListSize = dbJobIdList.size();
            }
        }
        LOGGER.info("Init get server url [{}], dbJobIdList size = {}",
                getServerNameListByIpUrl, dbJobIdListSize);
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
            clusterInfo.setServerVersion(jobHaDispatcher.getDbJobIdListVersion());
            reportTaskRequest.setDbSyncCluster(clusterInfo);
        }

        // handle error task config
        initTaskRegisterResults();
        reportTaskRequest.setTaskInfoList(new ArrayList<>(taskRegisterResults.values()));

        reportTaskRequest.setServerNames(jobManager.getCurrentRunDbJobIdList());

        String jobConfigString = httpManager.doSentPost(reportTaskConfigPeriodUrl, reportTaskRequest);
        CommonResponse<DbSyncTaskFullInfo> commonResponse =
                CommonResponse.fromJson(jobConfigString, DbSyncTaskFullInfo.class);
        parseJobAndCheckForStart(commonResponse);
        return true;
    }

    /**
     * get all taskInfos of one server Id for run jobs
     *
     * @param dbJobId dbJobId
     */
    public int initTaskInfoByDbJobId(String dbJobId, Integer dbsyncClusterId, Integer configVersion) throws Exception {
        int taskConfigSize = -1;
        DbSyncClusterInfo clusterInfo = new DbSyncClusterInfo(dbsyncClusterId, clusterName, configVersion);
        RunningTaskRequest runningTaskRequest = new RunningTaskRequest(localIp, clusterTag, clusterName, clusterInfo,
                dbJobId);

        String jobConfigString = httpManager.doSentPost(getTaskConfigByIpAndServerIdUrl, runningTaskRequest);
        CommonResponse<DbSyncTaskFullInfo> commonResponse =
                CommonResponse.fromJson(jobConfigString, DbSyncTaskFullInfo.class);
        if (commonResponse == null || !parseJobAndCheckForStart(commonResponse)) {
            throw new Exception("Get task's configs has error by server Id! dbJobId = "
                    + dbJobId + ", cluster id = " + dbsyncClusterId);
        }
        List<DbSyncTaskInfo> data = commonResponse.getData().getTaskInfoList();
        if (data != null) {
            taskConfigSize = data.size();
        }
        LOGGER.info("change run node Id  dbJobId[{}] , [{}], taskConfigSize = {}", dbJobId,
                getTaskConfigByIpAndServerIdUrl, taskConfigSize);
        return taskConfigSize;
    }

    public String getRegisterKey() {
        return localIp + ":" + agentConf.get(AGENT_UNIQ_ID, "1");
    }

    public String getLocalIp() {
        return localIp;
    }

    public String getAgentUniq() {
        return agentConf.get(AGENT_UNIQ_ID, "1");
    }

    private boolean parseSyncInfoAndRegisterInfo(CommonResponse<DbSyncInitInfo> commonResponse) {
        boolean result = false;
        if (commonResponse != null) {
            if (checkTdmReturnMsg(commonResponse)) {
                DbSyncInitInfo dbJobIdListInfo = commonResponse.getData();
                if (dbJobIdListInfo != null && StringUtils.isNotEmpty(dbJobIdListInfo.getZkUrl())) {

                    DbSyncClusterInfo clusterInfo = dbJobIdListInfo.getCluster();
                    Integer parentId = clusterInfo.getParentId();
                    Integer dbJobIdListVersion = clusterInfo.getServerVersion();
                    List<String> dbJobIdList = dbJobIdListInfo.getServerNames();
                    result = initAndRegisterInfoToZk(parentId, dbJobIdListVersion, dbJobIdListInfo.getZkUrl(),
                            dbJobIdList);
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
                                "ha op Task has task conf info error! ip:" + localIp, 1, tc.getVersion()));
                LOGGER.error("Error task config dbJobId {}, taskId {}", tc.getServerName(), tc.getId());
            }
        }
        List<DbSyncTaskInfo> correctTaskConfigList = jobHaDispatcher.getCorrectTaskConfInfList();
        if (correctTaskConfigList != null && correctTaskConfigList.size() > 0) {
            for (DbSyncTaskInfo tc : correctTaskConfigList) {
                taskRegisterResults.put(tc.getId(),
                        new TaskInfoBean(tc.getId(), tc.getStatus(), "config success! ip: " + localIp, 0,
                                tc.getVersion()));
                LOGGER.info("task config success dbJobId {}, taskId {}!", tc.getServerName(), tc.getId());
            }
        }
        List<DbSyncTaskInfo> exceedTaskConfigList = jobHaDispatcher.getExceedTaskInfList();
        if (exceedTaskConfigList != null && exceedTaskConfigList.size() > 0) {
            for (DbSyncTaskInfo tc : exceedTaskConfigList) {
                taskRegisterResults.put(tc.getId(),
                        new TaskInfoBean(tc.getId(), tc.getStatus(), "exceed job max limit, ip: " + localIp, -1,
                                tc.getVersion()));
                LOGGER.error("task config error dbJobId {}, taskId{}! exceed max job", tc.getServerName(), tc.getId());
            }
        }
    }

    /**
     * handler severIdListVersion or clusterId change
     *
     * @param newClusterId
     * @param newDbJobIdListVersion
     * @param zkUrl
     * @param dbJobIdList
     */
    private boolean initAndRegisterInfoToZk(Integer newClusterId,
            Integer newDbJobIdListVersion,
            String zkUrl,
            List<String> dbJobIdList) {
        return handlerDbJobIdListVersionChange(newClusterId, newDbJobIdListVersion, zkUrl, dbJobIdList);
    }

    /**
     * handler severIdListVersion or clusterId change
     *
     * @param newClusterId
     * @param newDbJobIdListVersion
     * @param zkUrl
     * @param dbJobIdList
     */
    private boolean handlerDbJobIdListVersionChange(Integer newClusterId,
            Integer newDbJobIdListVersion,
            String zkUrl,
            List<String> dbJobIdList) {
        return jobHaDispatcher.updateDbJobIdList(newClusterId, newDbJobIdListVersion, zkUrl,
                dbJobIdList, true);
    }

    public boolean parseJobAndCheckForStart(CommonResponse<DbSyncTaskFullInfo> commonResponse) {
        boolean result = false;
        if (checkTdmReturnMsg(commonResponse)) {
            taskRegisterResults.clear();
            DbSyncTaskFullInfo dbSyncTaskFullInfo = commonResponse.getData();
            if (dbSyncTaskFullInfo != null && StringUtils.isNotEmpty(dbSyncTaskFullInfo.getZkUrl())) {
                DbSyncClusterInfo clusterInfo = dbSyncTaskFullInfo.getCluster();
                String zkUrl = dbSyncTaskFullInfo.getZkUrl();
                if (clusterInfo != null) {
                    List<String> dbJobIdList = dbSyncTaskFullInfo.getChangedServers();
                    List<String> offlineServerIdList = dbSyncTaskFullInfo.getOfflineServers();
                    if (dbJobIdList != null && dbJobIdList.size() > 0) {
                        result = handlerDbJobIdListVersionChange(clusterInfo.getParentId(),
                                clusterInfo.getServerVersion(), zkUrl, dbJobIdList);
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
        List<DbSyncTaskInfo> changeTaskList = new ArrayList<>();
        while (iterator.hasNext()) {
            DbSyncTaskInfo taskConf = iterator.next();
            // LOGGER.warn("Object: {}, status :{}", taskConf, taskConf.getStatus());
            Integer status = taskConf.getStatus();
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                // 0 add; 4 stop; 5 run; 1 del; 2 modify
                if (status == ManagerOpEnum.ADD.getType()) {
                    if (checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.addJob(taskConf);
                        changeTaskList.add(taskConf);
                    }
                } else if (status == ManagerOpEnum.FROZEN.getType()) {
                    jobHaDispatcher.stopJob(taskConf.getServerName(), taskConf.getId(), taskConf);
                    future.complete(null);
                } else if (status == ManagerOpEnum.ACTIVE.getType()) {
                    if (checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.startJob(taskConf);
                        changeTaskList.add(taskConf);
                    }
                } else if (status == ManagerOpEnum.DEL.getType()) {
                    jobHaDispatcher.deleteJob(taskConf.getServerName(), taskConf.getId(), taskConf);
                    future.complete(null);
                } else if (status == ManagerOpEnum.RETRY.getType()) {
                    if (checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.updateJob(taskConf);
                        changeTaskList.add(taskConf);
                    }
                } else if (status == 101) {
                    if (checkTaskConfig(taskConf, future)) {
                        jobHaDispatcher.updateJob(taskConf);
                        changeTaskList.add(taskConf);
                    }
                } else {
                    LOGGER.error("Get task config in error status : {}", taskConf);
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
        return jobHaDispatcher.startAllTasksOnce(changeTaskList);
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
        return jobHaDispatcher.updateDbJobIdList(newClusterId, newServerIdListVersion, zkUrl,
                serverIdList, false);
    }
}
