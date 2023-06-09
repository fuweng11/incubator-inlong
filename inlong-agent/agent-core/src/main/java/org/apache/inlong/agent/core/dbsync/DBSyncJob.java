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

import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.core.DbAgentManager;
import org.apache.inlong.agent.core.task.Task;
import org.apache.inlong.agent.metrics.dbsync.StatisticInfo;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.plugin.AbstractJob;
import org.apache.inlong.agent.plugin.Channel;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Sink;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DBSyncJob implements AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncJob.class);
    private final DbAgentManager agentManager;
    private final DbAgentMetricManager dbSyncMetric;
    private final DbAgentHeartbeatManager heartbeatManager;
    private final ConcurrentHashMap<Integer, Task> dbSyncTasks;
    private final DBSyncJobConf dbSyncJobConf;

    private final String dbJobId;
    private final DbAgentReadJob readJob;

    public DBSyncJob(DbAgentManager agentManager, DBSyncJobConf dbSyncJobConf) {
        this.dbSyncJobConf = dbSyncJobConf;
        this.agentManager = agentManager;
        dbSyncTasks = new ConcurrentHashMap<>();
        dbJobId = this.dbSyncJobConf.getDbJobId();
        heartbeatManager = DbAgentHeartbeatManager.getInstance();
        dbSyncMetric = new DbAgentMetricManager(this);
        readJob = new DbAgentReadJob(this);
    }

    public DbAgentMetricManager getDBSyncMetric() {
        return dbSyncMetric;
    }

    public DbAgentReadJob getReadJob() {
        return readJob;
    }

    public Task getTaskById(Integer taskId) {
        return dbSyncTasks.get(taskId);
    }

    public DBSyncJobConf getDBSyncJobConf() {
        return dbSyncJobConf;
    }

    public String getDbJobId() {
        return dbJobId;
    }

    public JobStat.State getJobStat() {
        return readJob.getState();
    }

    public void start() {
        // create tasks and submit
        for (MysqlTableConf taskConf : dbSyncJobConf.getMysqlTableConfList()) {
            createAndAddTask(taskConf);
        }

        // make sure all tasks are inited finished before starting dbsyncReadOperator
        do {
            DBSyncUtils.sleep(1000);
        } while (!dbSyncTasks.values().stream().allMatch(Task::isTaskFinishInit));

        LOGGER.info("task{} init finished, start dbsync readJob!", dbSyncTasks.keySet());
        dbSyncMetric.start();
        readJob.start();

    }

    public void stop() {
        DbSyncHeartbeat stopHb = readJob.genHeartBeat(true);
        if (stopHb != null) {
            heartbeatManager.putStopHeartbeat(stopHb);
        }
        readJob.stop();

        if (dbSyncTasks != null) {
            dbSyncTasks.forEachKey(1, taskId -> {
                agentManager.getTaskManager().removeTask(String.valueOf(taskId));
                dbSyncTasks.remove(taskId);
            });
        }
        dbSyncMetric.stop();
    }

    public void createAndAddTask(MysqlTableConf taskConf) {
        JobProfile taskProfile = JobProfile.parseDbSyncTaskInfo(taskConf);
        Integer taskId = taskConf.getTaskId();
        LOGGER.info("job id: {}, create new taskId[{}], source: {}, channel: {}, sink: {}", taskProfile.getInstanceId(),
                taskId, taskProfile.get(JobConstants.JOB_SOURCE_CLASS),
                taskProfile.get(JobConstants.JOB_CHANNEL), taskProfile.get(JobConstants.JOB_SINK));

        if (dbSyncMetric != null && taskConf.getTaskInfo() != null
                && taskConf.getTaskInfo().getMqClusters() != null
                && taskConf.getTaskInfo().getMqClusters().size() >= 1) {
            dbSyncMetric.init(taskConf.getTaskInfo().getMqClusters().get(0).getUrl(),
                    this.dbSyncJobConf.getDbJobId());
        }

        try {
            Source source = (Source) Class.forName(taskProfile.get(JobConstants.JOB_SOURCE_CLASS)).newInstance();
            for (Reader reader : source.split(taskProfile)) {
                Sink writer = (Sink) Class.forName(taskProfile.get(JobConstants.JOB_SINK)).newInstance();
                writer.setSourceName(reader.getReadSource());
                Channel channel = (Channel) Class.forName(taskProfile.get(JobConstants.JOB_CHANNEL)).newInstance();
                taskProfile.set(reader.getReadSource(), DigestUtils.md5Hex(reader.getReadSource()));
                Task task = new Task(String.valueOf(taskId), reader, writer, channel, taskProfile, this);
                agentManager.getTaskManager().submitTask(task);
                dbSyncTasks.put(taskId, task);
                LOGGER.info("task [{}-{}] create and start success", dbJobId, taskId);
            }
        } catch (Throwable e) {
            LOGGER.error("create task[{}-{}] failed", dbJobId, taskId, e);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            throw new RuntimeException(e);
        }
    }

    protected void sendMetricPositionRecord(LogPosition newestLogPosition, LogPosition sendPosition,
            LogPosition oldestLogPosition) {
        Collection<MysqlTableConf> col = dbSyncJobConf.getMysqlTableConfList();
        JobStat.State state = getJobStat();
        String jobStat = state == null ? "" : state.name();
        DbAgentMetricManager dbAgentMetricManager = getDBSyncMetric();
        for (MysqlTableConf mysqlTableConf : col) {
            StatisticInfo info =
                    new StatisticInfo(mysqlTableConf.getGroupId(), mysqlTableConf.getStreamId(),
                            System.currentTimeMillis(), sendPosition,
                            dbJobId, dbJobId);
            if (dbAgentMetricManager != null) {
                dbAgentMetricManager.sendMetricsToPulsar(info, 0, newestLogPosition, oldestLogPosition, jobStat);
            }
        }
    }

    public CompletableFuture<Void> resetJob() {
        if (readJob != null) {
            return readJob.resetRead();
        }
        return null;
    }
}
