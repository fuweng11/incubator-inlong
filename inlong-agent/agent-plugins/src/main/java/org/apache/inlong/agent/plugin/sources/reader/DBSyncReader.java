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

package org.apache.inlong.agent.plugin.sources.reader;

import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.AbstractJob;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.JobConstants.DBSYNC_TASK_ID;

public class DBSyncReader extends AbstractReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncReader.class);
    private final LinkedBlockingQueue<DBSyncMessage> messageQueue;
    private String taskId = "-1";
    private volatile boolean finished = false;

    public DBSyncReader(JobProfile taskConf) {
        messageQueue = new LinkedBlockingQueue<>(5000);// TODO:configurable in agent.properties
        taskId = taskConf.get(DBSYNC_TASK_ID, "-1");
    }

    @Override
    public Message read() {
        if (!messageQueue.isEmpty()) {
            return messageQueue.poll();
        }
        try {
            return messageQueue.poll(JobConstants.DEFAULT_JOB_READ_WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("read data get interruptted.", e);
        }
        return null;
    }

    @Override
    public void addMessage(Message message) {
        if (message == null) {
            return;
        }
        try {
            readerMetric.pluginReadCount.incrementAndGet();
            messageQueue.put((DBSyncMessage) message);
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                    System.currentTimeMillis(), 1, message.getBody().length);
            readerMetric.pluginReadSuccessCount.incrementAndGet();
        } catch (Throwable e) {
            LOGGER.error("put message to dbsyncReader queue error", e);
            readerMetric.pluginReadFailCount.incrementAndGet();
        }
    }

    @Override
    public void init(JobProfile jobConf) {
        super.init(jobConf);
        LOGGER.info("dbsync-reader init finished, groupId[{}], streamId[{}], taskId[{}]", inlongGroupId, inlongStreamId,
                taskId);
    }

    @Override
    public void init(JobProfile jobConf, AbstractJob job) {

    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public String getReadSource() {
        return taskId;
    }

    @Override
    public void setReadTimeout(long mill) {

    }

    @Override
    public void setWaitMillisecond(long millis) {

    }

    @Override
    public String getSnapshot() {
        return null;
    }

    @Override
    public void finishRead() {
        while (!messageQueue.isEmpty()) {
            DBSyncUtils.sleep(500);
        }
        finished = true;
    }

    @Override
    public boolean isSourceExist() {
        return true;
    }

    @Override
    public void destroy() {

    }
}
