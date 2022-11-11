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

import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.task.Task;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DBSyncJob extends Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncJob.class);
    private final AgentManager agentManager;
    protected DBSyncJobConf dbSyncJobConf;
    protected String jobName;
    private Task task;

    public DBSyncJob(AgentManager agentManager, JobProfile dbSyncJobConf) {
        super(dbSyncJobConf);
        this.dbSyncJobConf = dbSyncJobConf.getDbSyncJobConf();
        this.agentManager = agentManager;
        jobName = this.dbSyncJobConf.getJobName();
    }

    public DBSyncJobConf getDBSyncJobConf() {
        return dbSyncJobConf;
    }

    public void stop() {
        if (task != null) {
            task.getReader().finishRead();
        }
    }

    public String getTaskId() {
        if (task != null) {
            return task.getTaskId();
        }
        return null;
    }

    public JobStat.State getJobStat() {
        if (task == null) {
            LOGGER.error("dbsync job {} is null", jobName);
            return State.ERROR;
        }
        return task.getReader().getState();
    }

    public void start() {
        task = super.createTask(jobConf);
        if (task != null) {
            agentManager.getTaskManager().submitTask(task);
        }
    }

    public CompletableFuture<Void> resetJob() {
        if (task != null && task.getReader() != null) {
            return task.getReader().resetReader();
        }
        return null;
    }
}
