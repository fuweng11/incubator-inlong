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

package org.apache.inlong.agent.core.job;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.db.JobProfileDb;
import org.apache.inlong.agent.db.StateSearchKey;
import org.apache.inlong.agent.except.DataSourceConfigException.InvalidCharsetNameException;
import org.apache.inlong.agent.except.DataSourceConfigException.JobSizeExceedMaxException;
import org.apache.inlong.agent.metrics.AgentMetricItem;
import org.apache.inlong.agent.metrics.AgentMetricItemSet;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.State;
import org.apache.inlong.agent.state.JobStat.TaskStat;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.GsonUtil;
import org.apache.inlong.agent.utils.JsonUtils.JSONArray;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RESETTING_CHECK_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_JOB_DB_CACHE_CHECK_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_JOB_DB_CACHE_TIME;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_JOB_NUMBER_LIMIT;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_RESETTING_CHECK_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.JOB_DB_CACHE_CHECK_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.JOB_DB_CACHE_TIME;
import static org.apache.inlong.agent.constant.AgentConstants.JOB_NUMBER_LIMIT;
import static org.apache.inlong.agent.constant.JobConstants.JOB_ID;
import static org.apache.inlong.agent.constant.JobConstants.JOB_ID_PREFIX;
import static org.apache.inlong.agent.constant.JobConstants.JOB_INSTANCE_ID;
import static org.apache.inlong.agent.constant.JobConstants.SQL_JOB_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_COMPONENT_NAME;

/**
 * JobManager maintains lots of jobs, and communicate between server and task manager.
 */
public class JobManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
    // jobs which are not accepted by running pool.
    private final ConcurrentHashMap<String, Job> pendingJobs;
    // job thread pool
    private final ThreadPoolExecutor runningPool;
    private final AgentManager agentManager;
    private final int monitorInterval;
    private final long jobDbCacheTime;
    private final long jobDbCacheCheckInterval;
    // job profile db is only used to recover instance which is not finished running.
    private final JobProfileDb jobProfileDb;
    private final AtomicLong index = new AtomicLong(0);
    private final long jobMaxSize;
    // key is job instance id.
    private final ConcurrentHashMap<String, JobWrapper> jobs;
    // metrics
    private final AgentMetricItemSet jobMetrics;
    private final Map<String, String> dimensions;

    private final AgentConfiguration agentConf;
    private final int maxConDbSize;
    private final JobConfManager jobConfManager;
    private final ConcurrentHashMap<String, DBSyncJob> allJobs; // TODO:merge to jobs
    private final ArrayList<DBSyncJobConf> newJobs;
    private final ReentrantReadWriteLock.WriteLock wLock;
    private final ConcurrentHashMap<String, DBSyncJob> runningJobs;

    /**
     * init job manager
     *
     * @param agentManager agent manager
     */
    public JobManager(AgentManager agentManager, JobProfileDb jobProfileDb) {
        this.jobConfManager = new JobConfManager();
        this.jobProfileDb = jobProfileDb;
        this.agentManager = agentManager;
        // job thread pool for running
        this.runningPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new AgentThreadFactory("job"));
        this.jobs = new ConcurrentHashMap<>();
        this.allJobs = new ConcurrentHashMap<>();
        this.runningJobs = new ConcurrentHashMap<>();
        this.newJobs = new ArrayList<>();
        this.agentConf = AgentConfiguration.getAgentConf();
        this.maxConDbSize = agentConf.getDbSyncMaxConDbSize();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.wLock = lock.writeLock();
        this.pendingJobs = new ConcurrentHashMap<>();
        this.monitorInterval = agentConf
                .getInt(
                        AgentConstants.JOB_MONITOR_INTERVAL, AgentConstants.DEFAULT_JOB_MONITOR_INTERVAL);
        this.jobDbCacheTime = agentConf.getLong(JOB_DB_CACHE_TIME, DEFAULT_JOB_DB_CACHE_TIME);
        this.jobDbCacheCheckInterval = agentConf.getLong(JOB_DB_CACHE_CHECK_INTERVAL,
                DEFAULT_JOB_DB_CACHE_CHECK_INTERVAL);
        this.jobMaxSize = agentConf.getLong(JOB_NUMBER_LIMIT, DEFAULT_JOB_NUMBER_LIMIT);

        this.dimensions = new HashMap<>();
        this.dimensions.put(KEY_COMPONENT_NAME, this.getClass().getSimpleName());
        this.jobMetrics = new AgentMetricItemSet(this.getClass().getSimpleName());
        MetricRegister.register(jobMetrics);
    }

    public List<String> getCurrentRunSyncIdList() {
        List<String> set = runningJobs.values().stream().map((job) -> (job.getDBSyncJobConf().getServerId()))
                .filter(StringUtils::isNotEmpty).collect(Collectors.toList());
        return set;
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

    public boolean isRunningJob(String syncId) {
        if (StringUtils.isNotEmpty(syncId)) {
            List<String> list =
                    runningJobs.values().stream().map((job) -> job.getDBSyncJobConf().getServerId()).collect(
                            Collectors.toList());
            if (list != null && list.contains(syncId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * submit job to work thread.
     *
     * @param job job
     */
    private void addJob(Job job) {
        if (pendingJobs.containsKey(job.getJobInstanceId())) {
            return;
        }
        try {
            JobWrapper jobWrapper = new JobWrapper(agentManager, job);
            JobWrapper jobWrapperRet = jobs.putIfAbsent(jobWrapper.getJob().getJobInstanceId(), jobWrapper);
            if (jobWrapperRet != null) {
                LOGGER.warn("{} has been added to running pool, "
                        + "cannot be added repeatedly", job.getJobInstanceId());
                return;
            } else {
                getJobMetric().jobRunningCount.incrementAndGet();
            }
            this.runningPool.execute(jobWrapper);
        } catch (Exception rje) {
            LOGGER.debug("reject job {}", job.getJobInstanceId(), rje);
            pendingJobs.putIfAbsent(job.getJobInstanceId(), job);
        } catch (Throwable t) {
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), t);
        }
    }

    /**
     * add file job profile
     *
     * @param profile job profile.
     */
    public boolean submitFileJobProfile(JobProfile profile) {
        return submitJobProfile(profile, false);
    }

    /**
     * add file job profile
     *
     * @param profile job profile.
     */
    public boolean submitJobProfile(JobProfile profile, boolean singleJob) {
        if (!isJobValid(profile)) {
            return false;
        }
        String jobId = profile.get(JOB_ID);
        if (singleJob) {
            profile.set(JOB_INSTANCE_ID, AgentUtils.getSingleJobId(JOB_ID_PREFIX, jobId));
        } else {
            profile.set(JOB_INSTANCE_ID, AgentUtils.getUniqId(JOB_ID_PREFIX, jobId, index.incrementAndGet()));
        }
        LOGGER.info("submit job profile {}", profile.toJsonStr());
        getJobConfDb().storeJobFirstTime(profile);
        addJob(new Job(profile));
        return true;
    }

    private boolean isJobValid(JobProfile profile) {
        if (profile == null || !profile.allRequiredKeyExist()) {
            LOGGER.error("profile is null or not all required key exists {}", profile == null ? null
                    : profile.toJsonStr());
            return false;
        }
        if (isJobOverLimit()) {
            LOGGER.error("agent cannot add more job, max job size is {}", jobMaxSize);
            return false;
        }
        return true;
    }

    /**
     * whether job size exceeds maxSize
     */
    public boolean isJobOverLimit() {
        return jobs.size() >= jobMaxSize;
    }

    /**
     * delete job profile and stop job thread
     *
     * @param jobInstancId
     */
    public boolean deleteJob(String jobInstancId) {
        LOGGER.info("start to delete job, job id set {}", jobs.keySet());
        JobWrapper jobWrapper = jobs.remove(jobInstancId);
        if (jobWrapper != null) {
            LOGGER.info("delete job instance with job id {}", jobInstancId);
            jobWrapper.cleanup();
            getJobConfDb().deleteJob(jobInstancId);
            return true;
        }
        return false;
    }

    // TODO: merge to deleteJob?
    public void stopJob(String jobName) {
        DBSyncJob job = runningJobs.get(jobName);
        if (job == null) {
            return;
        }
        int retryDelCnt = 0;
        do {
            job.stop();
            LOGGER.debug("get {} delete cmd, retry delete task!", jobName);
            DBSyncUtils.sleep(1000);
            retryDelCnt++;
        } while (job.getJobStat() != JobStat.State.STOP && retryDelCnt < 20);
        if (job.getJobStat() != JobStat.State.STOP) {
            LOGGER.error("stop {} task, after stop, state error {}", jobName, job.getJobStat());
        }

        runningJobs.remove(jobName);
        for (String taskId : job.getTaskId()) {
            agentManager.getTaskManager().removeTask(taskId);
        }
        allJobs.remove(jobName);
        jobConfManager.removeConf(job.getDBSyncJobConf());
        LOGGER.info("delete job[{}] complete success!", jobName);
    }

    /**
     * add a new task to job
     *
     * @param taskConf config
     * @param future future
     */
    public synchronized DBSyncJob addDbSyncTask(DbSyncTaskInfo taskConf, CompletableFuture<Void> future) {
        LOGGER.info("DbSyncTaskInfo is {}", taskConf);
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
            LOGGER.error("invalid charset name: {}, {}", taskConf.getCharset(), e.getMessage());
            future.completeExceptionally(
                    new InvalidCharsetNameException("invalid charset name " + taskConf.getCharset()));
            return null;
        }

        String instName = taskConf.getDbServerInfo() + ":" + taskConf.getServerName();
        LOGGER.debug("taskId[{}], add instance name :{}, dbName:{}, tbName:{}", taskConf.getId(), instName,
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
                JSONObject obj = JSONObject.parseObject(taskConf.getStartPosition());
                startPosition = new LogPosition(obj);
                LOGGER.info("taskId[{}]startPosition set to {} ", taskConf.getId(), startPosition);
            } catch (Throwable t) {
                LOGGER.error("parse start position error, startPosition set to null.", t);
            }
        }

        boolean isFound = false;
        DBSyncJobConf conf = jobConfManager.getConfByTaskId(taskConf.getId());
        if (conf == null) {
            conf = jobConfManager.getConfigByDatabase(taskConf.getDbServerInfo().getUrl(), taskConf.getServerName());
            if (conf == null) {
                // find back
                if (bakDbUrl != null) {
                    conf = jobConfManager.getConfigByDatabase(bakDbUrl, taskConf.getServerName());
                }
            }

            // can't find both master and bak
            if (conf == null) {
                // add new conf
                int dumpedSize = jobConfManager.getConfSize();
                if (dumpedSize >= maxConDbSize) {
                    LOGGER.error("skip job add, now dumped job size is {}, max job size {}, "
                            + "skipped config is {}, jobConfMng is {} ",
                            dumpedSize, maxConDbSize, taskConf.toString(), jobConfManager.toString());
                    future.completeExceptionally(
                            new JobSizeExceedMaxException("exceed max job size " + maxConDbSize));
                    return null;
                }

                String tmpUrl = taskConf.getDbServerInfo().getUrl();
                conf = new DBSyncJobConf(DBSyncUtils.getHost(tmpUrl), DBSyncUtils.getPort(tmpUrl),
                        taskConf.getDbServerInfo().getUsername(), taskConf.getDbServerInfo().getPassword(), charset,
                        startPosition, taskConf.getServerName());
                conf.setMaxUnackedLogPositions(agentConf.getInt(DBSYNC_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD,
                        DEFAULT_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD));
                if (bakDbUrl != null) {
                    conf.setBakMysqlInfo(DBSyncUtils.getHost(bakDbUrl), DBSyncUtils.getPort(bakDbUrl));
                }
                newJobs.add(conf);
                jobConfManager.putConf(instName, conf);
            } else {
                isFound = true;
            }
        } else {
            isFound = true;
        }

        if (isFound) {
            // update conf
            conf.updateUserPasswd(taskConf.getDbServerInfo().getUsername(),
                    taskConf.getDbServerInfo().getPassword());
            conf.updateCharset(charset, taskConf.getId());
            // allJobs.get(dbsyncJobConf.getJobName()).updateJobConf();//TODO:update charset
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
            MysqlTableConf tbConf = new MysqlTableConf(conf.getJobName(), taskConf, charset, skipDelete);
            conf.addTable(tbConf);
            conf.getMysqlTableConfList(dbName, taskConf.getTableName())
                    .forEach(myconf -> myconf.updateJobStatus(TaskStat.NORMAL));
            if (runningJobs.containsKey(conf.getJobName())) {
                runningJobs.get(conf.getJobName()).createAndAddTask(tbConf);
            }
        } else {
            // TODO:improve, update other info, such as table change
            MysqlTableConf mysqlTableConf = conf.getMysqlTableConf(taskConf.getId());
            if (mysqlTableConf != null) {
                mysqlTableConf.setSkipDelete(skipDelete);
            }
            LOGGER.warn("dbName {}, tableName {} already in conf, taskId {}", dbName, tableName, taskConf.getId());
        }

        final DBSyncJobConf finalConf = conf;
        allJobs.computeIfAbsent(conf.getJobName(), (key) -> new DBSyncJob(agentManager, finalConf));

        future.complete(null);
        return allJobs.get(conf.getJobName());
    }

    public synchronized void updateJob(String newJobName, String oldJobName, TaskStat stat) {
        try {
            // lock jobMap
            wLock.lock();
            DBSyncJobConf tmpJobConf = jobConfManager.getParsingConfigByInstName(newJobName);
            if (tmpJobConf == null) {
                LOGGER.error("find newJobName {}, old job Name {} null, pls check",
                        newJobName, oldJobName);
                return;
            }
            tmpJobConf.setStatus(stat);
        } finally {
            wLock.unlock();
        }
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

        DBSyncJobConf conf = jobConfManager.getConfByTaskId(taskId);
        if (conf == null) {
            LOGGER.warn("can't find task_id {} ", taskId);
            // not exist, delete ok!
            future.complete(null);
        } else {
            String jobName = conf.getJobName();
            conf.removeTable(taskId);
            agentManager.getTaskManager().removeTask(String.valueOf(taskId));
            if (conf.bNoNeedDb()) {
                stopJob(jobName);
            }
            future.complete(null);
        }
    }

    public String getJobNameByTaskId(Integer taskId) {
        DBSyncJobConf conf = jobConfManager.getConfByTaskId(taskId);
        if (conf != null) {
            return conf.getJobName();
        }
        return null;
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
                String jobName = job.getDBSyncJobConf() == null ? "" : job.getDBSyncJobConf().getJobName();
                JSONArray taskIds = job.getDBSyncJobConf() == null ? null : job.getDBSyncJobConf().getTasksJson();
                if (StringUtils.isNotEmpty(jobName)) {
                    runningJobs.compute(jobName, (k, v) -> {
                        if (v == null) {
                            LOGGER.info("Start new job after init, jobName={}, conf={}, "
                                    + "taskIdsSize = {}, taskIdList = {}",
                                    jobName,
                                    job.getDBSyncJobConf(),
                                    (taskIds == null ? 0 : taskIds.size()),
                                    (taskIds == null ? "" : taskIds.toJSONString()));
                            job.start();
                            return job;
                        }
                        JSONArray vTaskIds =
                                (v.getDBSyncJobConf() == null ? null : v.getDBSyncJobConf().getTasksJson());
                        LOGGER.info("Job {} has started! taskIdsSize = {}, taskIdList = {}",
                                jobName, (vTaskIds == null ? 0 : vTaskIds.size()),
                                (vTaskIds == null ? "" : vTaskIds.toJSONString()));
                        return v;
                    });
                }
            }
        }
    }

    /**
     * start all accepted jobs.
     */
    private void startJobs() {
        List<JobProfile> profileList = getJobConfDb().getRestartJobs();
        for (JobProfile profile : profileList) {
            LOGGER.info("init starting job from db {}", profile.toJsonStr());
            addJob(new Job(profile));
        }
    }

    /**
     * check pending jobs and submit them
     */
    public Runnable jobStateCheckThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    // check pending jobs and try to submit again.
                    for (String jobId : pendingJobs.keySet()) {
                        Job job = pendingJobs.remove(jobId);
                        if (job != null) {
                            addJob(job);
                        }
                    }
                    TimeUnit.SECONDS.sleep(monitorInterval);
                } catch (Throwable ex) {
                    LOGGER.error("error caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    /**
     * check local db and delete old tasks.
     */
    public Runnable dbStorageCheckThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    jobProfileDb.removeExpireJobs(jobDbCacheTime);
                    // TODO: manager handles those job state in the future and it's saved locally now.
                    Map<String, List<String>> jobStateMap = jobProfileDb.getJobsState();
                    LOGGER.info("check local job state: {}", GsonUtil.toJson(jobStateMap));
                } catch (Exception ex) {
                    LOGGER.error("removeExpireJobs error caught", ex);
                }
                try {
                    TimeUnit.SECONDS.sleep(jobDbCacheCheckInterval);
                } catch (Throwable ex) {
                    LOGGER.error("sleep error caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    /**
     * handle dbsync-job resetting operation
     */
    private Runnable getJobResettingCheckTask() {
        return () -> {
            int resettingCheckInterval = agentConf.getInt(DBSYNC_RESETTING_CHECK_INTERVAL,
                    DEFAULT_RESETTING_CHECK_INTERVAL);

            while (isRunnable()) {
                try {
                    for (Map.Entry<String, DBSyncJob> runningJob : runningJobs.entrySet()) {
                        DBSyncJob job = runningJob.getValue();
                        DBSyncJobConf jobConf = jobConfManager.getParsingConfigByInstName(runningJob.getKey());
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
     * mark job as success by job id.
     *
     * @param jobId job id
     */
    public void markJobAsSuccess(String jobId) {
        JobWrapper wrapper = jobs.remove(jobId);
        if (wrapper != null) {
            getJobMetric().jobRunningCount.decrementAndGet();
            LOGGER.info("job instance {} is success", jobId);
            // mark job as success.
            jobProfileDb.updateJobState(jobId, StateSearchKey.SUCCESS);
        }
    }

    /**
     * remove job from jobs, and mark it as failed
     *
     * @param jobId job id
     */
    public void markJobAsFailed(String jobId) {
        JobWrapper wrapper = jobs.remove(jobId);
        if (wrapper != null) {
            LOGGER.info("job instance {} is failed", jobId);
            getJobMetric().jobRunningCount.decrementAndGet();
            getJobMetric().jobFatalCount.incrementAndGet();
            // mark job as success.
            jobProfileDb.updateJobState(jobId, StateSearchKey.FAILED);
        }
    }

    public JobProfileDb getJobConfDb() {
        return jobProfileDb;
    }

    /**
     * check job existence using job file name
     */
    public boolean checkJobExist(String fileName) {
        return jobProfileDb.getJobByFileName(fileName) != null;
    }

    /**
     * get sql job existence
     */
    public boolean sqlJobExist() {
        return jobProfileDb.getJobById(SQL_JOB_ID) != null;
    }

    public Map<String, JobWrapper> getJobs() {
        return jobs;
    }

    @Override
    public void start() {
        submitWorker(jobStateCheckThread());
        submitWorker(dbStorageCheckThread());
        if (agentConf.enableHA()) {
            submitWorker(getJobResettingCheckTask());
        }
        startJobs();
    }

    @Override
    public void stop() throws Exception {
        stopDbSyncJobs();
        waitForTerminate();
        this.runningPool.shutdown();
    }

    private void stopDbSyncJobs() {
        if (!agentConf.enableHA()) {
            return;
        }
        LOGGER.info("begin to stop all dbsyncJobs");
        boolean stopped = false;
        while (!stopped) {
            stopped = true;
            for (Entry<String, DBSyncJob> entry : runningJobs.entrySet()) {
                if (entry.getValue().getJobStat() != State.STOP) {
                    stopped = false;
                    try {
                        LOGGER.info("retry stop dbsyncJob[{}] ", entry.getKey());
                        entry.getValue().stop();
                    } catch (Exception e) {
                        LOGGER.error("stop dbsyncJob[{}] error", entry.getKey());
                    }
                } else {
                    runningJobs.remove(entry.getKey());
                }
            }
            if (!stopped) {
                AgentUtils.silenceSleepInMs(1000 * 5);
            }
        }
        LOGGER.info("complete to stop all dbsyncJobs");
    }

    private AgentMetricItem getJobMetric() {
        return this.jobMetrics.findMetricItem(dimensions);
    }
}
