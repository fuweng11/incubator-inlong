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
import org.apache.inlong.agent.core.dbsync.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.task.ITaskPositionManager;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.metrics.MetricReport;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.JsonUtils;
import org.apache.inlong.agent.utils.MonitorLogUtils;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.AgentConstants.*;

public class DbAgentJobManager extends AbstractDaemon implements ITaskPositionManager, MetricReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbAgentJobManager.class);
    private static final Logger jobReportLogger = LoggerFactory.getLogger("jobReport");
    private static final Logger jobManagerReportLogger = LoggerFactory.getLogger("jobManagerReport");
    private static final Logger jobTaskLogger = LoggerFactory.getLogger("jobTaskMonitor");

    private final int maxConDbSize;
    private final ConcurrentHashMap<String, DBSyncJob> allJobs; // TODO:merge to jobs
    private final ArrayList<DBSyncJobConf> newJobs;
    private final DbAgentManager agentManager;
    private final AgentConfiguration agentConf;
    private final ConcurrentHashMap<String, DBSyncJob> runningJobs;
    private final int updateZkInterval;
    public DbAgentJobManager(DbAgentManager agentManager) {
        this.agentManager = agentManager;
        this.allJobs = new ConcurrentHashMap<>();
        this.runningJobs = new ConcurrentHashMap<>();
        this.newJobs = new ArrayList<>();
        this.agentConf = AgentConfiguration.getAgentConf();
        this.maxConDbSize = agentConf.getDbSyncMaxConDbSize();
        this.updateZkInterval = agentConf.getInt(DBSYNC_UPDATE_POSITION_INTERVAL, DEFAULT_UPDATE_POSITION_INTERVAL);
    }

    public void stopJob(String dbJobId) {
        DBSyncJob job = runningJobs.get(dbJobId);
        if (job == null) {
            LOGGER.warn("Get db Job by id [{}] is null!", dbJobId);
            return;
        }
        int retryDelCnt = 0;
        do {
            job.stop();
            DBSyncUtils.sleep(1000);
            retryDelCnt++;
        } while (job.getJobStat() != JobStat.State.STOP && retryDelCnt < 60);
        if (job.getJobStat() != JobStat.State.STOP) {
            LOGGER.error("Stop dbJob {}, after 60s, state error {}", dbJobId, job.getJobStat());
        }
        runningJobs.remove(dbJobId);
        allJobs.remove(dbJobId);
        LOGGER.info("Stop job[{}] complete success!", dbJobId);
    }

    /**
     * add a new task to job
     *
     * @param taskConf config
     * @param future future
     */
    public synchronized DBSyncJob addDbSyncTask(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        jobTaskLogger.info("Status: {},Object: {}", taskConf.getStatus(), taskConf);
        if (!DBSyncUtils.checkValidateDbInfo(taskConf, future)) {
            return null;
        }

        String bakDbUrl = null;
        if (StringUtils.isNotBlank(taskConf.getDbServerInfo().getBackupUrl())) {
            bakDbUrl = taskConf.getDbServerInfo().getBackupUrl();
        }

        Charset charset = null;
        try {
            if (StringUtils.isNotBlank(taskConf.getCharset())) {
                charset = Charset.forName(taskConf.getCharset());
            } else {
                charset = StandardCharsets.UTF_8;
            }
        } catch (Exception e) {
            LOGGER.error("dbJobId: [{}] invalid charset name: {}, {}", taskConf.getServerName(), taskConf.getCharset(),
                    e.getMessage());
            future.completeExceptionally(
                    new DataSourceConfigException.InvalidCharsetNameException(
                            "invalid charset name " + taskConf.getCharset()));
            return null;
        }

        LOGGER.info("TaskId[{}], add dbJobId :{}, dbName:{}, tbName:{}", taskConf.getId(), taskConf.getServerName(),
                taskConf.getDbName(), taskConf.getTableName());

        boolean skipDelete = false;
        if (taskConf.getSkipDelete() != null) {
            if (taskConf.getSkipDelete() == 1) {
                skipDelete = true;
            }
        }

        LogPosition startPosition = null;
        if (StringUtils.isNotBlank(taskConf.getStartPosition())) {
            try {
                JsonUtils.JSONObject obj = JsonUtils.JSONObject.parseObject(taskConf.getStartPosition());
                startPosition = new LogPosition(obj);
                LOGGER.info("taskId[{}]startPosition set to {} ", taskConf.getId(), startPosition);
            } catch (Throwable t) {
                LOGGER.error("parse start position error, startPosition set to null.", t);
            }
        }

        boolean isFound = false;
        DBSyncJob dbSyncJob = allJobs.get(taskConf.getServerName());
        DBSyncJobConf conf = (dbSyncJob == null ? null : dbSyncJob.getDBSyncJobConf());
        if (conf == null) {
            // add new conf
            int jobSize = allJobs.size();
            if (jobSize >= maxConDbSize) {
                LOGGER.error("skip job add, now dumped job size is {}, max job size {}, "
                        + "skipped config is {}, jobConfMng is {} ", jobSize, maxConDbSize, taskConf);
                future.completeExceptionally(
                        new DataSourceConfigException.JobSizeExceedMaxException("exceed max job size " + maxConDbSize));
                return null;
            }

            String masterUrl = taskConf.getDbServerInfo().getUrl();
            String backupUrl = taskConf.getDbServerInfo().getBackupUrl();
            conf = new DBSyncJobConf(DBSyncUtils.getHost(masterUrl), DBSyncUtils.getPort(masterUrl),
                    DBSyncUtils.getHost(backupUrl), DBSyncUtils.getPort(backupUrl),
                    taskConf.getDbServerInfo().getUsername(), taskConf.getDbServerInfo().getPassword(), charset,
                    startPosition, taskConf.getServerName());
            conf.setMaxUnAckedLogPositions(agentConf.getInt(DBSYNC_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD,
                    DEFAULT_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD));
            if (bakDbUrl != null) {
                conf.setBakMysqlInfo(DBSyncUtils.getHost(bakDbUrl), DBSyncUtils.getPort(bakDbUrl));
            }
            newJobs.add(conf);
        } else {
            isFound = true;
        }

        if (isFound) {
            // update conf
            conf.updateUserPasswd(taskConf.getDbServerInfo().getUsername(),
                    taskConf.getDbServerInfo().getPassword());
            conf.updateCharset(charset, taskConf.getId());
            String dbUrl = taskConf.getDbServerInfo().getUrl();
            try {
                conf.resetDbInfo(dbUrl, bakDbUrl);
            } catch (Exception e) {
                LOGGER.error("exception occurred when reset: ", e);
            }
        }

        Integer taskId = taskConf.getId();
        String dbName = taskConf.getDbName();
        String tableName = taskConf.getTableName();
        if (!conf.containsTask(taskId)) {
            MysqlTableConf tbConf = new MysqlTableConf(conf.getDbJobId(), taskConf, charset, skipDelete);
            if (runningJobs.containsKey(conf.getDbJobId())) {
                runningJobs.get(conf.getDbJobId()).createAndAddTask(tbConf);
            }
            conf.addTable(tbConf);
            conf.getMysqlTableConfList(dbName, taskConf.getTableName())
                    .forEach(myconf -> myconf.updateJobStatus(JobStat.TaskStat.NORMAL));
        } else {
            // TODO:improve, update other info, such as table change
            MysqlTableConf mysqlTableConf = conf.getMysqlTableConf(taskConf.getId());
            if (mysqlTableConf != null) {
                mysqlTableConf.setSkipDelete(skipDelete);
            }
            LOGGER.warn("dbName {}, tableName {} already in conf, taskId {}", dbName, tableName, taskConf.getId());
        }

        final DBSyncJobConf finalConf = conf;
        allJobs.computeIfAbsent(conf.getDbJobId(), (key) -> new DBSyncJob(agentManager, finalConf));

        future.complete(null);
        return allJobs.get(conf.getDbJobId());
    }

    /**
     * delelte the task from job, if job not db data need dump, stop the job
     *
     * @param taskConf
     */
    public synchronized void deleteTask(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        Integer taskId = taskConf.getId();
        String dbName = taskConf.getDbName();
        String tableName = taskConf.getTableName();

        LOGGER.debug("Get taskId :{}, dbName:{}, tbName:{} to delete task!", taskId, dbName, tableName);
        DBSyncJob dbSyncJob = allJobs.get(taskConf.getServerName());
        DBSyncJobConf conf = (dbSyncJob == null ? null : dbSyncJob.getDBSyncJobConf());
        if (conf == null) {
            LOGGER.warn("can't find task_id {} ", taskId);
            // not exist, delete ok!
            future.complete(null);
        } else {
            String jobName = conf.getDbJobId();
            conf.removeTable(taskId);
            if (conf.bNoNeedDb()) {
                stopJob(jobName);
            }
            future.complete(null);
        }
    }

    public void checkAndStopJobForce(String jobName) {
        DBSyncJob job = runningJobs.get(jobName);
        if (job == null) {
            return;
        }
        DBSyncJobConf conf = job.getDBSyncJobConf();
        if (!conf.bNoNeedDb()) {
            LOGGER.warn("There are some task is not stop in Job! {}", conf.getMysqlTableConfList());
        }
        stopJob(jobName);
    }

    /**
     * start all init jobs when starting dbsync to avoid data loss when task are
     * send at different time
     */
    public void startAllInitJobs(List<DBSyncJob> newJobs) {
        if (newJobs != null && newJobs.size() > 0) {
            LOGGER.info("start new jobs {}", newJobs.size());
            for (DBSyncJob job : newJobs) {
                String jobName = job.getDBSyncJobConf() == null ? "" : job.getDBSyncJobConf().getDbJobId();
                List taskIds = job.getDBSyncJobConf() == null ? null : job.getDBSyncJobConf().getTaskIdList();
                if (StringUtils.isNotEmpty(jobName)) {
                    runningJobs.compute(jobName, (k, v) -> {
                        if (v == null) {
                            LOGGER.info("Start new job after init, jobName={}, conf={}, "
                                    + "taskIdsSize = {}, taskIdList = {}",
                                    jobName,
                                    job.getDBSyncJobConf(),
                                    (taskIds == null ? 0 : taskIds.size()),
                                    (taskIds == null ? "" : StringUtils.join(taskIds, ",")));
                            job.start();
                            return job;
                        }
                        List vTaskIds =
                                (v.getDBSyncJobConf() == null ? null : v.getDBSyncJobConf().getTaskIdList());
                        LOGGER.info("Job {} has started! taskIdsSize = {}, taskIdList = {}",
                                jobName, (vTaskIds == null ? 0 : vTaskIds.size()),
                                (vTaskIds == null ? "" : StringUtils.join(vTaskIds, ",")));
                        return v;
                    });
                }
            }
        }
    }

    private void stopDbSyncJobs() {
        /*
         * start stop
         */
        LOGGER.info("Begin stop all dbJob! [{}]", runningJobs.keySet());
        List<CompletableFuture> futures = runningJobs.entrySet().stream()
                .map((entry) -> stopDbJobAsync(entry.getValue())).collect(Collectors.toList());
        CompletableFuture allOf =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        allOf.join();
        LOGGER.info("Complete to stop all dbsync jobs!");
    }

    private CompletableFuture stopDbJobAsync(DBSyncJob dbSyncJob) {
        return CompletableFuture.supplyAsync(() -> {
            if (dbSyncJob.getJobStat() != JobStat.State.STOP) {
                try {
                    LOGGER.info("retry stop dbJobId[{}] ", dbSyncJob.getDbJobId());
                    try {
                        stopJob(dbSyncJob.getDbJobId());
                    } catch (Exception e) {
                        LOGGER.error("stop job by dbJobId = {} has exception e = {}", dbSyncJob.getDbJobId(), e);
                    }
                } catch (Exception e) {
                    LOGGER.error("stop dbsyncJob[{}] error", dbSyncJob.getDbJobId());
                }
            }
            return true;
        }).thenAccept(u -> {
            LOGGER.info("Stop job dbJobId = {} finished!", dbSyncJob.getDbJobId());
        }).exceptionally(ex -> {
            LOGGER.error("stop job dbJobId = {} finished has exception e = {}",
                    dbSyncJob.getDbJobId(), ex);
            return null;
        });
    }

    @Override
    public void stop() throws Exception {
        stopDbSyncJobs();
        waitForTerminate();
    }

    @Override
    public void start() {
        submitWorker(getJobResettingCheckTask());
        submitWorker(dbSyncPositionUpdateThread());
    }

    public List<String> getCurrentRunDbJobIdList() {
        List<String> set = runningJobs.values().stream().map((job) -> (job.getDBSyncJobConf().getDbJobId()))
                .filter(StringUtils::isNotEmpty).collect(Collectors.toList());
        return set;
    }

    public boolean isRunningJob(String dbJobId) {
        if (StringUtils.isNotEmpty(dbJobId)) {
            List<String> list =
                    runningJobs.values().stream().map((job) -> job.getDBSyncJobConf().getDbJobId()).collect(
                            Collectors.toList());
            if (list != null && list.contains(dbJobId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the position control of job; backup job should be considered
     */
    private ReadJobPositionManager getJobPositionManager(String jobName) {
        DBSyncJob dbSyncJob = allJobs.get(jobName);
        if (dbSyncJob != null) {
            return dbSyncJob.getReadJob().getJobPositionManager();
        } else {
            LOGGER.warn("[{}] get get job position manager!", jobName);
        }
        return null;
    }

    @Override
    public void updateSinkPosition(BatchProxyMessage batchMsg, String sourcePath, long size) {
        String jobName = batchMsg.getJobId();
        ReadJobPositionManager readJobPositionManager = this.getJobPositionManager(jobName);

        if (readJobPositionManager != null) {
            readJobPositionManager.ackSendPosition(batchMsg);
        } else {
            LOGGER.warn("[{}] can not find position while update sink ack msg! {}", jobName, batchMsg.getPositions());
        }
    }

    @Override
    public void updateSinkPosition(String jobInstanceId, String sourcePath, long size, boolean reset) {

    }

    public DBSyncJob getJob(String jobName) {
        return allJobs.get(jobName);
    }

    public ConcurrentHashMap<String, DBSyncJob> getRunningJobs() {
        return runningJobs;
    }

    public ConcurrentHashMap<String, DBSyncJob> getAllJobs() {
        return allJobs;
    }

    /**
     * 检查是否需要做reset 操作
     */
    private Runnable getJobResettingCheckTask() {
        return () -> {
            int resettingCheckInterval = agentConf.getInt(DBSYNC_RESETTING_CHECK_INTERVAL,
                    DEFAULT_RESETTING_CHECK_INTERVAL);
            while (isRunnable()) {
                try {
                    for (Map.Entry<String, DBSyncJob> runningJob : runningJobs.entrySet()) {
                        DBSyncJob job = runningJob.getValue();
                        if (job == null) {
                            continue;
                        }
                        DBSyncJobConf jobConf = job.getDBSyncJobConf();
                        if (jobConf != null) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("current mysql Ip = {}/{} ,contain = {}", jobConf.getCurMysqlIp(),
                                        jobConf.getCurMysqlPort(), jobConf.containsDatabase(jobConf.getCurMysqlUrl()));
                            }
                            if (job != null && (!jobConf.containsDatabase(jobConf.getCurMysqlUrl()))) {
                                if (jobConf.getStatus() == JobStat.TaskStat.SWITCHING) {
                                    LOGGER.warn("Job [{}] is switching!, so skip for resetting!", runningJob.getKey());
                                } else {
                                    CompletableFuture<Void> future = job.resetJob();
                                    if (future != null) {
                                        future.whenCompleteAsync((ign, t) -> {
                                            if (t != null) {
                                                LOGGER.error("job[{}] has exception while resetting:",
                                                        runningJob.getKey(), t);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                    TimeUnit.SECONDS.sleep(resettingCheckInterval);
                } catch (Throwable e) {
                    LOGGER.error("getJobResettingCheckTask has exception ", e);
                }
            }
        };
    }

    /**
     *  更新每个job 的同步 位点 到ha，ha 模块 异步更新到zk
     */
    private Runnable dbSyncPositionUpdateThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    /*
                     * When there is no table name or field name matching rule, it needs to be updated with the position
                     * parsed by binlog
                     */
                    for (Map.Entry<String, DBSyncJob> runningJob : runningJobs.entrySet()) {
                        DBSyncJob dbSyncJob = runningJob.getValue();
                        String jobName = runningJob.getKey();
                        if (dbSyncJob == null) {
                            LOGGER.warn("runningJob posControl [{}] is null!", jobName);
                            continue;
                        }
                        ReadJobPositionManager readJobPositionManager =
                                runningJob.getValue().getReadJob().getJobPositionManager();
                        LogPosition storePos = readJobPositionManager.getSendAndAckedLogPosition();
                        if (storePos == null) {
                            LOGGER.warn("runningJob [{}] store position is null", jobName);
                            continue;
                        }
                        readJobPositionManager.updateSendAndAckedPosition(storePos,
                                dbSyncJob.getReadJob().getPkgIndexId());
                    }
                    TimeUnit.SECONDS.sleep(updateZkInterval);
                } catch (Throwable e) {
                    LOGGER.error("getPositionUpdateTask has exception ", e);
                }
            }
        };
    }

    @Override
    public String report() {
        StringBuilder bs = new StringBuilder();
        Set<String> allJobsKeys = new HashSet<>(allJobs.keySet());
        Set<String> runJobsKeys = runningJobs.keySet();
        int allJobsSize = allJobsKeys.size();
        int runningJobsSize = runJobsKeys.size();
        int unRunJobSize = 0;
        allJobsKeys.removeAll(runJobsKeys);
        if (allJobsKeys.size() > 0) {
            unRunJobSize = allJobsKeys.size();
        }

        /*
         * all job manager metric
         */
        JobHaDispatcherImpl jobHaDispatcher = JobHaDispatcherImpl.getInstance();
        String jobCoordinatorIp = "";
        if (jobHaDispatcher != null) {
            jobCoordinatorIp = jobHaDispatcher.getJobCoordinatorIp();
        }
        bs.append("JobCoordinator:").append(jobCoordinatorIp).append("|")
                .append("All-jobs:").append(allJobsSize).append("|")
                .append("Run-jobs:").append(runningJobsSize).append("|")
                .append("Un-run-job:").append(unRunJobSize).append("|")
                .append("Un-run-job-list:[").append(StringUtils.join(allJobsKeys, ",")).append("]|");
        jobManagerReportLogger.info(bs.toString());

        /*
         * single job metric
         */
        for (Map.Entry<String, DBSyncJob> entry : runningJobs.entrySet()) {
            bs = new StringBuilder();
            bs.append("JobName:" + entry.getKey()).append("|");
            DBSyncJob tmpJob = entry.getValue();

            DbAgentReadJob dbAgentReadJob = tmpJob.getReadJob();
            ReadJobPositionManager readJobPositionManager = dbAgentReadJob.getJobPositionManager();

            DbAgentMetricManager dbAgentMetricManager = tmpJob.getDBSyncMetric();
            if (dbAgentMetricManager != null) {
                bs.append(dbAgentMetricManager.report()).append("|");
            }
            bs.append(readJobPositionManager.report());
            jobReportLogger.info(bs.toString());
            MonitorLogUtils.printDumpMetric("JobName:" + entry.getKey(), dbAgentReadJob.report());
        }
        return "";
    }
}
