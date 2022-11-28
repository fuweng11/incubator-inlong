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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.dbsync.DBSyncReadOperator;
import org.apache.inlong.agent.core.task.Task;
import org.apache.inlong.agent.plugin.Channel;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Sink;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DBSyncJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncJob.class);
    private final AgentManager agentManager;
    private final ConcurrentHashMap<Integer, Task> dbSyncTasks;
    protected DBSyncJobConf dbSyncJobConf;
    protected String jobName;
    private DBSyncReadOperator readOperator;

    public DBSyncJob(AgentManager agentManager, DBSyncJobConf dbSyncJobConf) {
        this.dbSyncJobConf = dbSyncJobConf;
        this.agentManager = agentManager;
        dbSyncTasks = new ConcurrentHashMap<>();
        jobName = this.dbSyncJobConf.getJobName();
        readOperator = new DBSyncReadOperator(this);
    }

    public Task getTaskById(Integer taskId) {
        return dbSyncTasks.get(taskId);
    }

    public DBSyncJobConf getDBSyncJobConf() {
        return dbSyncJobConf;
    }

    // TODO: when task is delete
    public void removeDBSyncTask(Integer taskId) {
        dbSyncTasks.remove(taskId);
    }

    public void stop() {
        readOperator.stop();
    }

    public Set<String> getTaskId() {
        return dbSyncTasks.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
    }

    public JobStat.State getJobStat() {
        return readOperator.getState();
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

        LOGGER.info("task{} init finished, start dbsyncReadOperator", dbSyncTasks.keySet());
        readOperator.start();
    }

    public void createAndAddTask(MysqlTableConf taskConf) {
        JobProfile taskProfile = JobProfile.parseDbSyncTaskInfo(taskConf);
        Integer taskId = taskConf.getTaskId();
        LOGGER.info("job id: {}, create new taskId[{}], source: {}, channel: {}, sink: {}", taskProfile.getInstanceId(),
                taskId, taskProfile.get(JobConstants.JOB_SOURCE_CLASS),
                taskProfile.get(JobConstants.JOB_CHANNEL), taskProfile.get(JobConstants.JOB_SINK));
        try {
            Source source = (Source) Class.forName(taskProfile.get(JobConstants.JOB_SOURCE_CLASS)).newInstance();
            for (Reader reader : source.split(taskProfile)) {
                Sink writer = (Sink) Class.forName(taskProfile.get(JobConstants.JOB_SINK)).newInstance();
                writer.setSourceName(reader.getReadSource());
                Channel channel = (Channel) Class.forName(taskProfile.get(JobConstants.JOB_CHANNEL)).newInstance();
                taskProfile.set(reader.getReadSource(), DigestUtils.md5Hex(reader.getReadSource()));
                Task task = new Task(String.valueOf(taskId), reader, writer, channel, taskProfile);
                dbSyncTasks.put(taskId, task);
                agentManager.getTaskManager().submitTask(task);
                LOGGER.info("task [{}-{}] create and start success", jobName, taskId);
            }
        } catch (Throwable e) {
            LOGGER.error("create task[{}-{}] failed", jobName, taskId, e);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            throw new RuntimeException(e);
        }

    }

    public CompletableFuture<Void> resetJob() {
        if (readOperator != null) {
            return readOperator.resetRead();
        }
        return null;
    }
}
