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

package org.apache.inlong.agent.metrics.dbsync;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.entites.JobMetricInfo;
import org.apache.inlong.agent.utils.DBSyncUtils;

import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_BLOCKING_QUEUE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_COMPRESSION_TYPE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_ENABLE_BATCHING;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_MAX_BATCHING_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_PULSAR_TOPIC;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_SEND_THREAD_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_BLOCKING_QUEUE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_COMPRESSION_TYPE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_ENABLE_BATCHING;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_MAX_BATCHING_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_PULSAR_TOPIC;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_SEND_THREAD_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_ASYNC;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_BUSY_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_ASYNC;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_BUSY_SIZE;

public class DbSyncMetricSink {

    private static final Logger logger = LogManager.getLogger(DbSyncMetricSink.class);
    private static final Logger jobSenderReportLogger = LogManager.getLogger("jobSenderReport");
    public static final String DATA_KEY_DELIMITER = "#";

    // private DBSyncConf config;
    private LinkedBlockingQueue<Object> sendQueue;

    /*
     * key instName/jobName, value: topic
     */
    private ArrayList<SenderRunner> senderThreadList;

    /*
     * for control
     */
    private Semaphore semaphore;
    private int monitorInterval;
    private int pulsarBusySize;

    /*
     * pulsar client
     */
    private PulsarClient client;
    private String clusterUrl;
    private int clientIoThreadNum;
    private int connectionsPreBroker;
    private int pendingNum;
    private int maxPendingMessagesAcrossPartitions;
    private int batchSize;
    private boolean enableBatch;
    private long maxBatchingPublishDelayMillis;
    private boolean blockQueue;
    private CompressionType compressionType;

    private boolean async = true;

    // private boolean isMetric;
    private volatile boolean running = true;

    private String urlPrefix;

    private String serverName;
    // private AbstractJob job;

    private String pulsarMetricsTopic;

    private ScheduledExecutorService producerMonitorExecutor;
    private AtomicLong sendingCnt = new AtomicLong(0L);

    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();

    public DbSyncMetricSink(String clusterUrl) {
        this.clusterUrl = clusterUrl;
        this.urlPrefix = (clusterUrl == null ? "" : clusterUrl.substring(0, 20));
        logger.info("Initial pulsar [{}]", clusterUrl);
    }

    public void start(String serverName) {
        logger.info("Pulsar sender begin work!");
        this.serverName = serverName;
        int sendThreadSize = agentConf.getInt(DBSYNC_SEND_THREAD_SIZE, DEFAULT_DBSYNC_SEND_THREAD_SIZE);
        int pulsarSendQueueSize =
                agentConf.getInt(DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE, DEFAULT_DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE);

        this.semaphore = new Semaphore(pulsarSendQueueSize);
        this.clientIoThreadNum =
                agentConf.getInt(DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM, DEFAULT_DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM);
        this.connectionsPreBroker = agentConf.getInt(DBSYNC_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER,
                DEFAULT_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER);
        this.monitorInterval = agentConf.getInt(DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES,
                DEFAULT_DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES);

        pendingNum = agentConf.getInt(DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES,
                DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES);

        maxPendingMessagesAcrossPartitions =
                agentConf.getInt(DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS,
                        DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);

        maxBatchingPublishDelayMillis = agentConf.getInt(DBSYNC_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS,
                DEFAULT_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS);

        pulsarMetricsTopic = agentConf.get(DBSYNC_PULSAR_TOPIC, DEFAULT_DBSYNC_PULSAR_TOPIC);

        batchSize = agentConf.getInt(DBSYNC_PULSAR_MAX_BATCHING_SIZE,
                DEFAULT_DBSYNC_PULSAR_MAX_BATCHING_SIZE);

        enableBatch = agentConf.getBoolean(DBSYNC_PULSAR_ENABLE_BATCHING, DEFAULT_DBSYNC_PULSAR_ENABLE_BATCHING);
        blockQueue = agentConf.getBoolean(DBSYNC_PULSAR_BLOCKING_QUEUE, DEFAULT_DBSYNC_PULSAR_BLOCKING_QUEUE);
        async = agentConf.getBoolean(PULSAR_ASYNC, DEFAULT_PULSAR_ASYNC);

        compressionType = DBSyncUtils
                .convertType(agentConf.get(DBSYNC_PULSAR_COMPRESSION_TYPE, DEFAULT_DBSYNC_PULSAR_COMPRESSION_TYPE));
        pulsarBusySize = agentConf.getInt(PULSAR_BUSY_SIZE, DEFAULT_PULSAR_BUSY_SIZE);

        sendQueue = new LinkedBlockingQueue<>(pulsarSendQueueSize);
        senderThreadList = new ArrayList<>();
        try {
            client = PulsarClient.builder()
                    .connectionsPerBroker(connectionsPreBroker)
                    .ioThreads(clientIoThreadNum)
                    .serviceUrl(clusterUrl)
                    .build();
        } catch (Exception e) {
            logger.error("init pulsar client fail", e);
        }

        for (int i = 0; i < sendThreadSize; i++) {
            SenderRunner sender = new SenderRunner();
            sender.setName("Pulsar-Metric-sender-" + i);
            senderThreadList.add(sender);
        }

        for (SenderRunner sender : senderThreadList) {
            sender.start();
        }

        producerMonitorExecutor = Executors
                .newSingleThreadScheduledExecutor(
                        new DefaultThreadFactory(serverName + "-producer-monitor"));
        producerMonitorExecutor.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    printMonitorInfo();
                } catch (Exception e) {
                    logger.error("Close producer has exception e = {}", e);
                }
            }
        }, 1, monitorInterval, TimeUnit.MINUTES);
    }

    private class SenderRunner extends Thread {

        private ConcurrentHashMap<String, Producer> senderMap = new ConcurrentHashMap<>();

        private volatile boolean runnerRunning = true;

        public String getMonitorInfo() {
            return this.getName() + ", topics numbers = " + senderMap.size() + ", send queue size"
                    + " = " + sendQueue.size();
        }

        @Override
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("start sender Runner");
            }
            while (running) {
                try {
                    Object data = sendQueue.poll(1, TimeUnit.MILLISECONDS);
                    if (data == null) {
                        if (!running) {
                            logger.info("Stop send Runner!");
                            break;
                        }
                        continue;
                    }
                    JobMetricInfo jobMetricInfo = (JobMetricInfo) data;
                    sendingMetric(jobMetricInfo,
                            senderMap.computeIfAbsent(pulsarMetricsTopic,
                                    k -> getProducer(pulsarMetricsTopic)));
                } catch (Throwable t) {
                    logger.error("Send data error", t);
                }
            }

            Set<Entry<String, Producer>> setEntries = senderMap.entrySet();
            for (Entry<String, Producer> entry : setEntries) {
                try {
                    if (entry.getValue() != null && entry.getValue().isConnected()) {
                        entry.getValue().close();
                    }
                } catch (Throwable e) {
                    logger.error("[{}] Close producer has exception", serverName, e);
                }
            }
            runnerRunning = false;
        }

        public boolean isRunnerRunning() {
            return runnerRunning;
        }
    }

    private void sendingMetric(JobMetricInfo data, Producer producer) throws InterruptedException {
        // sending data async
        if (producer == null) {
            logger.warn("[{}] producer is null!", pulsarMetricsTopic);
            sendQueue.put(data);
            return;
        }
        sendingCnt.incrementAndGet();
        if (async) {
            CompletableFuture<MessageId> future = producer
                    .newMessage()
                    .value(JSONObject.toJSONString(data).getBytes(StandardCharsets.UTF_8)).sendAsync();
            future.whenCompleteAsync((m, t) -> {
                // exception is not null, that means not success.
                if (t != null) {
                    logger.error("send data fail to pulsar,add back to send queue and"
                            + "current queue size {}", sendQueue.size(), t);
                    try {
                        sendQueue.put(data);
                    } catch (InterruptedException e) {
                        logger.error("put metrics to queue has exception:", e);
                    }
                } else {
                    semaphore.release();
                }
                sendingCnt.decrementAndGet();
            });
        } else {
            try {
                producer.newMessage()
                        .value(JSONObject.toJSONString(data).getBytes(StandardCharsets.UTF_8)).send();
                semaphore.release();
            } catch (PulsarClientException e) {
                logger.error("send data fail to pulsar,add back to send queue, send queue "
                        + "size {}", sendQueue.size(), e);
                try {
                    sendQueue.put(data);
                } catch (InterruptedException interruptedException) {
                    logger.error("put metrics to queue has exception:", e);
                }
            }
            sendingCnt.decrementAndGet();
        }
    }

    /**
     * get Pulsar producer
     *
     * @param pulsarTopic
     * @return
     */
    private Producer<byte[]> getProducer(String pulsarTopic) {
        try {
            logger.info("try to init producer, enableBatch = {}, batchSize = {}, blockQueue = {},"
                    + "pendingNum = {} ,compressionType = {} ,topic = {}",
                    enableBatch, batchSize, blockQueue,
                    pendingNum, compressionType, pulsarTopic);
            return client.newProducer().topic(pulsarTopic)
                    .compressionType(compressionType)
                    .batchingMaxBytes(batchSize)
                    .batchingMaxPublishDelay(maxBatchingPublishDelayMillis, TimeUnit.MILLISECONDS)
                    .blockIfQueueFull(blockQueue)
                    .maxPendingMessages(pendingNum)
                    .maxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions)
                    .enableBatching(enableBatch)
                    .create();
        } catch (Throwable e) {
            logger.error("[{}] Init Pulsar Producer exception: ", pulsarTopic, e);
            return null;
        }
    }

    private void printMonitorInfo() {
        for (SenderRunner runner : senderThreadList) {
            logger.info("[{}] Monitor info [{}]", serverName, runner.getMonitorInfo());
        }
    }

    public void stop() {
        while (!sendQueue.isEmpty() || sendingCnt.get() != 0) {
            try {
                logger.info("Now wait in logQueue:{}, sendingCnt:{}",
                        sendQueue.size(), sendingCnt.get());
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        running = false;
        while (true) {
            boolean isAllClosed = true;
            if (senderThreadList != null) {
                for (SenderRunner senderRunner : senderThreadList) {
                    if (senderRunner.isRunnerRunning()) {
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
        try {
            if (producerMonitorExecutor != null && !producerMonitorExecutor.isShutdown()) {
                producerMonitorExecutor.shutdown();
                producerMonitorExecutor = null;
            }
        } catch (Throwable e) {
            logger.error("[{}] producerMonitorExecutor shutdown has exception!", serverName, e);
        }

        try {
            if (client != null) {
                client.close();
                client = null;
            }
        } catch (Exception e) {
            logger.error("[{}] Close pulsar client has error !", serverName);
        }
    }

    // @Override
    public void sendData(Object data) {
        if (!running) {
            logger.warn("PulsarSender is not started or is stopped!");
            return;
        }
        try {
            semaphore.acquire();
            sendQueue.put(data);
        } catch (Exception e) {
            semaphore.release();
            logger.error("send data fail to send queue", e);
        }
    }

    public boolean isBusy() {
        return sendQueue.size() > pulsarBusySize;
    }

    public boolean isEmpty() {
        return sendQueue.isEmpty();
    }

    public String getMetric() {
        StringBuilder bs = new StringBuilder();
        bs.append("clusterUrlPrefix:").append(urlPrefix).append("|")
                .append("sendQueueSize:").append(sendQueue.size()).append("|")
                .append("semaphoreSize:").append(semaphore.availablePermits()).append("|")
                .append("isBusy:").append(isBusy()).append("|");
        return bs.toString();
    }

    public void report() {
        StringBuilder bs = new StringBuilder();
        bs.append("JobName:").append(serverName).append("|").append(getMetric());
        jobSenderReportLogger.info(bs.toString());
    }
}
