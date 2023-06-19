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

package org.apache.inlong.agent.plugin.sinks;

import org.apache.inlong.agent.core.dbsync.DbAgentMetricManager;
import org.apache.inlong.agent.core.task.ITaskPositionManager;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.metrics.AgentMetricItem;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.InLongMsg;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarSendThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSendThread.class);

    public static final String DATA_KEY_DELIMITER = "#";

    private static final AtomicInteger CLIENT_INDEX = new AtomicInteger(0);

    private volatile boolean shutdown = false;

    private LinkedBlockingQueue<BatchProxyMessage> pulsarSendQueue;

    private List<PulsarClient> pulsarClients;

    private AgentMetricItem sinkMetric;

    private DbAgentMetricManager dbSyncMetricJob;

    private ITaskPositionManager taskPositionManager;

    private PulsarSenderManager pulsarSenderManager;

    private Semaphore sendQueueSemaphore;

    private boolean asyncSend;

    private AtomicLong sendingCnt;

    private String sourceName;

    private String dbJobId;

    private String threadId;

    private volatile boolean threadRunning = false;

    public PulsarSendThread(LinkedBlockingQueue<BatchProxyMessage> pulsarSendQueue, List<PulsarClient> pulsarClients,
            AgentMetricItem sinkMetric, DbAgentMetricManager dbSyncMetricJob,
            ITaskPositionManager taskPositionManager, PulsarSenderManager pulsarSenderManager,
            Semaphore sendQueueSemaphore, AtomicLong sendingCnt,
            boolean asyncSend, String sourceName, String dbJobId, String threadID) {
        this.pulsarSendQueue = pulsarSendQueue;
        this.pulsarClients = pulsarClients;
        this.sinkMetric = sinkMetric;
        this.sendQueueSemaphore = sendQueueSemaphore;
        this.dbSyncMetricJob = dbSyncMetricJob;
        this.taskPositionManager = taskPositionManager;
        this.pulsarSenderManager = pulsarSenderManager;
        this.sendingCnt = sendingCnt;
        this.asyncSend = asyncSend;
        this.dbJobId = dbJobId;
        this.sourceName = sourceName;
        this.threadId = threadID;
        this.threadRunning = true;
    }

    @Override
    public void run() {
        LOGGER.info("start pulsar sink send data thread, job[{}] threadName = {}", dbJobId, this.getName());
        while (!shutdown) {
            try {
                BatchProxyMessage data = pulsarSendQueue.poll(1, TimeUnit.MILLISECONDS);
                if (ObjectUtils.isEmpty(data)) {
                    continue;
                }
                sendData(data);
            } catch (Throwable t) {
                LOGGER.error("Send data error", t);
            }
        }
        threadRunning = false;
    }

    private void sendData(BatchProxyMessage batchMsg) throws InterruptedException {
        if (ObjectUtils.isEmpty(batchMsg)) {
            return;
        }
        String topic = getTopicFromBatchMessage(batchMsg);
        Producer producer = selectProducer(dbJobId, topic);
        if (ObjectUtils.isEmpty(producer)) {
            pulsarSendQueue.put(batchMsg);
            LOGGER.error("send dbJobId = [{}] threadId = [{}] , topic = [{}] data err, empty pulsar producer",
                    dbJobId, threadId, topic);
            return;
        }
        InLongMsg message = batchMsg.getInLongMsg();
        sinkMetric.pluginSendCount.addAndGet(batchMsg.getMsgCnt());
        if (asyncSend) {
            CompletableFuture<MessageId> future = producer.newMessage().eventTime(batchMsg.getDataTime())
                    .value(message.buildArray()).sendAsync();
            future.whenCompleteAsync((m, t) -> {
                if (t != null) {
                    // send error
                    sinkMetric.pluginSendFailCount.addAndGet(batchMsg.getMsgCnt());
                    LOGGER.error("send data fail to pulsar, add back to send queue, current queue size {}",
                            pulsarSendQueue.size(), t);
                    try {
                        pulsarSendQueue.put(batchMsg);
                    } catch (InterruptedException e) {
                        LOGGER.error("put back to queue fail send queue size {}", pulsarSendQueue.size(), t);
                    }
                } else {
                    // send success, update metrics
                    sendQueueSemaphore.release();
                    sendingCnt.decrementAndGet();
                    updateSuccessSendMetrics(batchMsg);
                }
            });
        } else {
            try {
                producer.newMessage().eventTime(batchMsg.getDataTime()).value(message.buildArray()).send();
                sendQueueSemaphore.release();
                sendingCnt.decrementAndGet();
                updateSuccessSendMetrics(batchMsg);
            } catch (PulsarClientException e) {
                sinkMetric.pluginSendFailCount.addAndGet(batchMsg.getMsgCnt());
                LOGGER.error("send data fail to pulsar, add back to send queue, send queue size {}",
                        pulsarSendQueue.size(), e);
                pulsarSendQueue.put(batchMsg);
            }
        }
    }

    private Producer selectProducer(String dbJobId, String topic) {
        int currentIndex = CLIENT_INDEX.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            CLIENT_INDEX.set(0);
        }
        PulsarTopicSender sender = pulsarSenderManager.getPulsarTopicSender(currentIndex,
                dbJobId, threadId, topic, pulsarClients);
        return sender == null ? null : sender.getProducer();
    }

    private void updateSuccessSendMetrics(BatchProxyMessage batchMsg) {
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_SEND_SUCCESS, batchMsg.getGroupId(),
                batchMsg.getStreamId(), batchMsg.getDataTime(), batchMsg.getMsgCnt(),
                batchMsg.getTotalSize());
        sinkMetric.pluginSendSuccessCount.addAndGet(batchMsg.getMsgCnt());
        taskPositionManager.updateSinkPosition(batchMsg, sourceName, batchMsg.getMsgCnt());
        addSuccessMetrics(batchMsg);
    }

    /**
     * add to jobManager
     *
     * @param data
     */
    private void addSuccessMetrics(BatchProxyMessage data) {
        if (data == null || CollectionUtils.isEmpty(data.getPositions())) {
            return;
        }
        String groupId = data.getGroupId();
        String streamId = data.getStreamId();
        String dataKey = getDataKey(data);

        dbSyncMetricJob.addStatisticInfo(groupId, streamId,
                data.getDataTime(), data.getMsgCnt(),
                data.getPositions().get(0).getKey(), data.getJobId(), dataKey);
    }
    private String getDataKey(BatchProxyMessage data) {
        return String.join(DATA_KEY_DELIMITER, data.getGroupId(), data.getStreamId(),
                getTopicFromBatchMessage(data),
                String.valueOf(data.getDataTime()), data.getJobId());
    }

    private String getTopicFromBatchMessage(BatchProxyMessage batchMsg) {
        if (batchMsg != null && batchMsg.getExtraMap() != null) {
            return batchMsg.getExtraMap().get(AttributeConstants.MESSAGE_TOPIC);
        }
        return null;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public boolean isThreadRunning() {
        return threadRunning;
    }
}
