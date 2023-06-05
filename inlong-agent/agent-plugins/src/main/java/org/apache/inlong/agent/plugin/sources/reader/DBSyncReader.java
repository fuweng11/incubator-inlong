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

import org.apache.inlong.agent.conf.AgentConfiguration;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_SEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_SEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.JobConstants.DBSYNC_TASK_ID;

public class DBSyncReader extends AbstractReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncReader.class);
    private final LinkedBlockingQueue<DBSyncMessage> messageQueue;
    private String taskId = "-1";
    private volatile boolean finished = false;

    private Semaphore queueSemaphore;

    public DBSyncReader(JobProfile taskConf) {
        messageQueue = new LinkedBlockingQueue<>(5000);// TODO:configurable in agent.properties
        taskId = taskConf.get(DBSYNC_TASK_ID, "-1");
        queueSemaphore = new Semaphore(
                AgentConfiguration.getAgentConf().getInt(PULSAR_SINK_SEND_QUEUE_SIZE, DEFAULT_SEND_QUEUE_SIZE));
    }

    @Override
    public Message read() {
        Message message = null;
        try {
            message = messageQueue.poll(JobConstants.DEFAULT_JOB_READ_WAIT_TIMEOUT, TimeUnit.SECONDS);
            if (message != null) {
                queueSemaphore.release();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("read data get interrupted.", e);
            queueSemaphore.release();
        }
        return message;
    }

    @Override
    public void addMessage(Message message) {
        if (message == null) {
            return;
        }
        try {
            queueSemaphore.acquire();

            DBSyncMessage dbSyncMessage = (DBSyncMessage) message;
            readerMetric.pluginReadCount.incrementAndGet();
            messageQueue.put(dbSyncMessage);
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                    getMessageTimeStamp(dbSyncMessage), 1, dbSyncMessage.getBody().length);
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

    public Long getMessageTimeStamp(DBSyncMessage dbSyncMessage) {
        if (dbSyncMessage != null
                && dbSyncMessage.getMsgTimeStamp() != null && dbSyncMessage.getMsgTimeStamp() > 0) {
            return dbSyncMessage.getMsgTimeStamp();
        }
        return System.currentTimeMillis();
    }
}
