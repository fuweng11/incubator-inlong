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

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.core.dbsync.DBSyncJob;
import org.apache.inlong.agent.core.task.PositionManager;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.message.EndMessage;
import org.apache.inlong.agent.message.PackProxyMessage;
import org.apache.inlong.agent.message.ProxyMessage;
import org.apache.inlong.agent.plugin.AbstractJob;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.utils.PluginUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_ENABLE_ASYNC_SEND;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_SINK_MAX_CACHE_BYTES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_SINK_SEND_THREAD_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_SEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_ENABLE_ASYNC_SEND;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_ENABLE_DISCARD_EXCEED_MSG;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_FLUSH_CACHE_MAX_INTERVAL;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_FLUSH_CACHE_MAX_MSG_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_MAX_CACHE_BYTES;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_SEND_PACKAGE_WAIT_CNT;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_SEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_SINK_SEND_THREAD_SIZE;

public class PulsarSink extends AbstractSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerFactory.class);
    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
    private List<MQClusterInfo> mqClusterInfos;
    private ArrayList<PulsarSendThread> senderThreadList = new ArrayList<>();
    private DBSyncJob job;
    private Semaphore sendQueueSemaphore; // limit the count of batchProxyMessage waiting to be sent
    private LinkedBlockingQueue<BatchProxyMessage> pulsarSendQueue;
    private int sendThreadSize;
    private int sendQueueSize;
    private boolean enableDiscardExceedMsg;
    private PulsarSenderManager pulsarSenderManager;
    private List<PulsarClient> pulsarClients;
    private int waitCnt = 0;
    private int waitCntMaxNum = 5;
    private long lastFlushCache = 0l;
    private long flushCacheMaxInterval = 2 * 60 * 1000L;
    private long maxCacheByteSize = 1024 * 1024 * 1024;
    private int maxFlushMsgNumber = 64;
    private AtomicLong sendingCnt = new AtomicLong(0L);
    private final AtomicLong cacheMsgSize = new AtomicLong(0);
    private volatile Boolean isDestroyed = false;
    private AtomicBoolean isFlushing = new AtomicBoolean(false);

    @Override
    public void init(JobProfile jobConf, AbstractJob job) {
        super.init(jobConf);
        this.job = (DBSyncJob) job;
        this.jobInstanceId = ((DBSyncJob) job).getDbJobId();
        this.sendQueueSize = agentConf.getInt(PULSAR_SINK_SEND_QUEUE_SIZE, DEFAULT_SEND_QUEUE_SIZE);
        this.sendThreadSize = agentConf.getInt(PULSAR_SINK_SEND_THREAD_SIZE, DEFAULT_PULSAR_SINK_SEND_THREAD_SIZE);
        this.maxCacheByteSize = agentConf.getInt(PULSAR_SINK_MAX_CACHE_BYTES, DEFAULT_PULSAR_SINK_MAX_CACHE_BYTES);
        String en = jobConf.get(PULSAR_SINK_ENABLE_DISCARD_EXCEED_MSG);
        if ("false".equals(en)) {
            enableDiscardExceedMsg = false;
        } else {
            this.enableDiscardExceedMsg = agentConf.getBoolean(PULSAR_SINK_ENABLE_DISCARD_EXCEED_MSG, true);
        }
        this.waitCntMaxNum = agentConf.getInt(PULSAR_SINK_SEND_PACKAGE_WAIT_CNT, 5);
        this.flushCacheMaxInterval = agentConf.getLong(PULSAR_SINK_FLUSH_CACHE_MAX_INTERVAL, 2 * 60 * 1000L);
        this.maxFlushMsgNumber = agentConf.getInt(PULSAR_SINK_FLUSH_CACHE_MAX_MSG_NUM, 64);
        this.sendQueueSemaphore = new Semaphore(sendQueueSize);
        this.pulsarSendQueue = new LinkedBlockingQueue<>(sendQueueSize);
        // jobConf
        this.mqClusterInfos = jobConf.getMqClusters();
        this.pulsarSenderManager = new PulsarSenderManager(((DBSyncJob) job).getDbJobId());
        this.lastFlushCache = Instant.now().toEpochMilli();
        initPulsarSender();
    }

    @Override
    public void init(JobProfile jobConf) {
    }

    @Override
    public void write(Message message) {
        if (isDestroyed && !(message instanceof EndMessage)) {
            LOGGER.warn("This pulsar sink has destroyed {} , {}",
                    (job == null ? "" : job.getDbJobId()));
            return;
        }
        try {
            if (message != null) {
                if (!(message instanceof EndMessage)) {
                    ProxyMessage proxyMessage = new ProxyMessage(message);
                    // add proxy message to cache.
                    cache.compute(proxyMessage.getBatchKey(),
                            (s, packProxyMessage) -> {
                                if (packProxyMessage == null) {
                                    packProxyMessage =
                                            new PackProxyMessage(jobInstanceId, jobConf,
                                                    PluginUtils.getGroupId(message),
                                                    PluginUtils.getStreamId(message));
                                    packProxyMessage.generateExtraMap(proxyMessage.getDataKey());
                                    packProxyMessage.addTopicAndDataTime(PluginUtils.getTopic(message),
                                            getDataTime(proxyMessage));
                                    packProxyMessage.setEnableDiscardExceedMsg(enableDiscardExceedMsg);
                                    packProxyMessage.setMaxFlushMsgNumber(maxFlushMsgNumber);
                                    if (StringUtils.isNotEmpty(proxyMessage.getTaskID())) {
                                        packProxyMessage.addTaskId(proxyMessage.getTaskID());
                                    }
                                }
                                // add message to package proxy
                                packProxyMessage.addProxyMessage(proxyMessage);
                                cacheMsgSize.addAndGet(proxyMessage.getBody().length);
                                flushPackMessage(packProxyMessage, false);
                                return packProxyMessage;
                            });
                    // increment the count of successful sinks
                    sinkMetric.sinkSuccessCount.incrementAndGet();
                } else {
                    // increment the count of failed sinks
                    sinkMetric.sinkFailCount.incrementAndGet();
                }
            } else {
                waitCnt++;
            }
            boolean isEnd = (message != null && message instanceof EndMessage);
            if (isNeedFlushCacheDate() || isEnd) {
                if (isEnd) {
                    LOGGER.info("Job [{}] end message for flush ALl", jobInstanceId);
                }
                flushCache(isEnd);
            }
        } catch (Throwable e) {
            LOGGER.error("write message to Proxy sink error", e);
        }
    }

    private boolean isNeedFlushCacheDate() {
        long nowTime = Instant.now().toEpochMilli();
        if (waitCnt >= waitCntMaxNum || (nowTime - lastFlushCache) > flushCacheMaxInterval
                || cacheMsgSize.get() > maxCacheByteSize) {
            lastFlushCache = Instant.now().toEpochMilli();
            waitCnt = 0;
            return true;
        }
        return false;
    }

    private long getDataTime(ProxyMessage proxyMessage) {
        long dataTime = Instant.now().toEpochMilli();
        if (proxyMessage != null && proxyMessage.getHeader() != null) {
            String dateStr = proxyMessage.getHeader().get(CommonConstants.PROXY_KEY_DATE);
            if (StringUtils.isNotEmpty(dateStr) && StringUtils.isNumeric(dateStr)) {
                try {
                    dataTime = Long.parseLong(dateStr);
                } catch (Exception e) {
                    LOGGER.error("parse proxy message date [{}] has exception!", dateStr, e);
                    dataTime = Instant.now().toEpochMilli();
                }
            } else {
                LOGGER.warn("proxy message date [{}] conf has error!", dateStr);
            }
        }
        return dataTime;
    }

    private void initPulsarSender() {
        if (CollectionUtils.isEmpty(mqClusterInfos)) {
            LOGGER.error("init job[{}] pulsar client fail, empty mqCluster info", jobInstanceId);
            return;
        }
        pulsarClients = pulsarSenderManager.getPulsarClients(job.getDbJobId(), mqClusterInfos);
        for (int i = 0; i < sendThreadSize; i++) {
            PulsarSendThread sender = new PulsarSendThread(pulsarSendQueue, pulsarClients, sinkMetric,
                    this.job.getDBSyncMetric(), PositionManager.getInstance(), pulsarSenderManager,
                    sendQueueSemaphore, sendingCnt,
                    agentConf.getBoolean(PULSAR_CLIENT_ENABLE_ASYNC_SEND, DEFAULT_ENABLE_ASYNC_SEND),
                    sourceName, job.getDbJobId(), String.valueOf(i));
            sender.setName("Pulsar-data-sender-" + i);
            sender.start();
            senderThreadList.add(sender);
        }
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy pulsar sink, job[{}], source[{}]", jobInstanceId, sourceName);
        isDestroyed = true;
        while (!sinkFinish()) {
            LOGGER.warn("job {} wait until cache all data to pulsar cache size = {}, "
                    + "sendingCnt = {}, pulsarSendQueue size = {}",
                    jobInstanceId, cache.size(), sendingCnt.get(), pulsarSendQueue.size());
            if (cache != null && cache.size() > 0) {
                flushCache(true);
            }
            AgentUtils.silenceSleepInMs(batchFlushInterval);
        }

        for (PulsarSendThread pulsarSendThread : senderThreadList) {
            pulsarSendThread.shutdown();
        }
        while (true) {
            boolean isAllClosed = true;
            if (senderThreadList != null) {
                for (PulsarSendThread pulsarSendThread : senderThreadList) {
                    if (pulsarSendThread.isThreadRunning()) {
                        isAllClosed = false;
                        break;
                    }
                }
            }
            if (!isAllClosed) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
                continue;
            }
            break;
        }
        senderThreadList.clear();
        if (job != null && pulsarSenderManager != null) {
            try {
                pulsarSenderManager.destoryPulsarTopicSender();
            } catch (Exception e) {
                LOGGER.error("[{}] destroy pulsarSender has Exception ", job.getDbJobId(), e);
            }
        }
        if (pulsarClients != null && pulsarSenderManager != null) {
            try {
                pulsarSenderManager.destoryPulsarClient();
            } catch (Exception e) {
                LOGGER.warn("[{}] destroy pulsar client has Exception ", job.getDbJobId(), e);
            }
        }
    }

    private boolean sinkFinish() {
        return cache.values().stream().allMatch(PackProxyMessage::isEmpty) && pulsarSendQueue.isEmpty()
                && (sendingCnt.get() == 0);
    }

    /**
     * flush cache by batch
     *
     * @return thread runner
     */
    private void flushPackMessage(PackProxyMessage packProxyMessage, boolean isFlushAll) {
        if (packProxyMessage != null) {
            long curTotal = packProxyMessage.getCurrentMessageQueueSize();
            BatchProxyMessage batchProxyMessage = packProxyMessage.fetchBatch(isFlushAll);
            do {
                if (batchProxyMessage != null) {
                    flushPackProxyMessage(batchProxyMessage);
                    batchProxyMessage = packProxyMessage.fetchBatch(isFlushAll);
                }
            } while (batchProxyMessage != null);
            long remainSize = packProxyMessage.getCurrentMessageQueueSize();
            cacheMsgSize.addAndGet(remainSize - curTotal);
        }
    }

    /**
     * flush cache by batch
     *
     * @return thread runner
     */
    private void flushCache(boolean isFlushAll) {
        List<String> emptyBatchKeyList = new ArrayList();
        if (isFlushing.compareAndSet(false, true)) {
            try {
                cache.forEach((batchKey, packProxyMessage) -> {
                    flushPackMessage(packProxyMessage, isFlushAll);
                    if (packProxyMessage.isEmpty()) {
                        emptyBatchKeyList.add(batchKey);
                    }
                });
                emptyBatchKeyList.forEach((key) -> {
                    cache.remove(key);
                });
            } finally {
                isFlushing.compareAndSet(true, false);
            }
        }
    }

    private void flushPackProxyMessage(BatchProxyMessage batchProxyMessage) {
        if (batchProxyMessage != null) {
            try {
                sendQueueSemaphore.acquire();
                pulsarSendQueue.put(batchProxyMessage);
                sendingCnt.incrementAndGet();
            } catch (Exception e) {
                sendQueueSemaphore.release();
                LOGGER.error("flush data to send queue", e);
            }
        }
    }

    @Override
    public String report() {
        return "cacheMsgSize:" + getFileSize(cacheMsgSize.get())
                + "|pulsarSendQueue:" + (pulsarSendQueue == null ? 0 : pulsarSendQueue.size())
                + "|sendingMsgSize:" + sendingCnt.get()
                + "|sendQueueSemaphore:" + (sendQueueSemaphore == null ? 0 : sendQueueSemaphore.availablePermits())
                + (pulsarSenderManager == null ? "" : ("|" + pulsarSenderManager.report()));
    }

    public String getFileSize(long size) {
        double length = size;

        if (length < 1024) {
            return length + "B";
        } else {
            length = length / 1024.0;
        }

        if (length < 1024) {
            return Math.round(length * 100) / 100.0 + "KB";
        } else {
            length = length / 1024.0;
        }
        if (length < 1024) {
            return Math.round(length * 100) / 100.0 + "MB";
        } else {
            return Math.round(length / 1024 * 100) / 100.0 + "GB";
        }
    }
}
