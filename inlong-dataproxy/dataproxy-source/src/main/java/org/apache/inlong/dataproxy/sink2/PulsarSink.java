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

package org.apache.inlong.dataproxy.sink2;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PulsarSink extends BaseSink {

    private static final Logger logger = LoggerFactory.getLogger(PulsarSink.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);
    private static final LogCounter logDupMsgPrinter = new LogCounter(10, 100000, 30 * 1000);

    private static final String MAX_BROKER_QUARANTINE_TIME_SEC = "max-broker-abnormal-quarantine-sec";
    private static final int VAL_DEF_BROKER_QUARANTINE_TIME_SEC = 300;
    private static final int VAL_MIN_BROKER_QUARANTINE_TIME_SEC = 0;

    private static String MAX_PULSAR_IO_THREAD_NUM = "max-pulsar-io-threads";
    private static final int VAL_DEF_MAX_PULSAR_IO_THREAD_NUM = 8;
    private static final int VAL_MIN_MAX_PULSAR_IO_THREAD_NUM = 1;

    private static String ENABLE_PRODUCER_BATCH = "enable-batch";
    private static final boolean VAL_DEF_ENABLE_PRODUCER_BATCH = true;

    private static String BLOCK_IF_PRODUCER_QUEUE_FULL = "block-if-queue-full";
    private static final boolean VAL_DEF_BLOCK_IF_P_QUEUE_FULL = true;

    private static String MAX_PRODUCER_PENDING_MESSAGES = "max-pending-messages";
    private static final int VAL_DEF_PRODUCER_PENDING_MESSAGES = 10000;
    private static final int VAL_MIN_PRODUCER_PENDING_MESSAGES = 0;

    private static String MAX_PRODUCER_BATCHING_MESSAGES = "max-batching-messages";
    private static final int VAL_DEF_PRODUCER_BATCHING_MESSAGES = 1000;
    private static final int VAL_MIN_PRODUCER_BATCHING_MESSAGES = 0;

    public PulsarClient pulsarClient;
    private int pulsarIOThreads;
    private long clientStatsPrintIntvlSec;
    private int abnQuarantineSec;
    private static final ConcurrentHashMap<String, Producer> producerMap = new ConcurrentHashMap<>();
    private boolean enableProducerBatch;
    private boolean blockIfProducerQueueFull;
    private int maxProducerPendingMsgs;
    private int maxProducerBatchingMsgs;
    // last refresh topics
    private final Set<String> lastRefreshTopics = new HashSet<>();
    // whether to send message
    private volatile boolean canSend = true;

    @Override
    public void configure(Context context) {
        super.configure(context);
        // get master address list
        this.clusterAddrList = context.getString(ConfigConstants.MASTER_SERVER_URL_LIST);
        Preconditions.checkState(clusterAddrList != null,
                ConfigConstants.MASTER_SERVER_URL_LIST + " parameter not specified");

        // get client statistic print interval in second
        this.clientStatsPrintIntvlSec = context.getLong(ConfigConstants.CLIENT_STATS_OUTPUT_INTVL_SEC,
                ConfigConstants.VAL_DEF_CLIENT_STATS_OUTPUT_INTVL_SEC);
        Preconditions.checkArgument(
                (this.clientStatsPrintIntvlSec >= ConfigConstants.VAL_MIN_CLIENT_STATS_OUTPUT_INTVL_SEC),
                ConfigConstants.CLIENT_STATS_OUTPUT_INTVL_SEC + " must be >= "
                        + ConfigConstants.VAL_MIN_CLIENT_STATS_OUTPUT_INTVL_SEC);

        this.pulsarIOThreads = context.getInteger(MAX_PULSAR_IO_THREAD_NUM, VAL_DEF_MAX_PULSAR_IO_THREAD_NUM);
        Preconditions.checkArgument((this.pulsarIOThreads >= VAL_MIN_MAX_PULSAR_IO_THREAD_NUM),
                MAX_PULSAR_IO_THREAD_NUM + " must be >= " + VAL_MIN_MAX_PULSAR_IO_THREAD_NUM);

        this.abnQuarantineSec = context.getInteger(MAX_BROKER_QUARANTINE_TIME_SEC,
                VAL_DEF_BROKER_QUARANTINE_TIME_SEC);
        Preconditions.checkArgument((this.abnQuarantineSec >= VAL_MIN_BROKER_QUARANTINE_TIME_SEC),
                MAX_BROKER_QUARANTINE_TIME_SEC + " must be >= " + VAL_MIN_BROKER_QUARANTINE_TIME_SEC);

        this.enableProducerBatch = context.getBoolean(ENABLE_PRODUCER_BATCH, VAL_DEF_ENABLE_PRODUCER_BATCH);
        this.blockIfProducerQueueFull =
                context.getBoolean(BLOCK_IF_PRODUCER_QUEUE_FULL, VAL_DEF_BLOCK_IF_P_QUEUE_FULL);
        this.maxProducerPendingMsgs =
                context.getInteger(MAX_PRODUCER_PENDING_MESSAGES, VAL_DEF_PRODUCER_PENDING_MESSAGES);
        Preconditions.checkArgument((this.maxProducerPendingMsgs >= VAL_MIN_PRODUCER_PENDING_MESSAGES),
                MAX_PRODUCER_PENDING_MESSAGES + " must be >= " + VAL_MIN_PRODUCER_PENDING_MESSAGES);
        this.maxProducerBatchingMsgs =
                context.getInteger(MAX_PRODUCER_BATCHING_MESSAGES, VAL_DEF_PRODUCER_BATCHING_MESSAGES);
        Preconditions.checkArgument((this.maxProducerBatchingMsgs >= VAL_MIN_PRODUCER_BATCHING_MESSAGES),
                MAX_PRODUCER_BATCHING_MESSAGES + " must be >= " + VAL_MIN_PRODUCER_BATCHING_MESSAGES);
    }

    @Override
    public void startSinkProcess() {
        logger.info("{} sink logic starting...", cachedSinkName);
        // register meta-configure listener to config-manager
        ConfigManager.getInstance().regPulsarXfeConfigChgCallback(this);
        try {
            this.pulsarClient = PulsarClient.builder()
                    .serviceUrl(this.clusterAddrList)
                    .ioThreads(this.pulsarIOThreads)
                    .statsInterval(this.clientStatsPrintIntvlSec, TimeUnit.SECONDS)
                    .connectionTimeout(this.connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .socketAddressQuarantineTimeSeconds(this.abnQuarantineSec)
                    .build();
        } catch (Throwable e) {
            stop();
            logger.error("{} create pulsar client failure, please re-check. ex2 {}",
                    cachedSinkName, e.getMessage());
            throw new FlumeException("create pulsar client failure, "
                    + "maybe tube master set error/shutdown in progress, please re-check");
        }
        // start message process logic
        for (int i = 0; i < sinkThreadPool.length; i++) {
            sinkThreadPool[i] = new Thread(new SinkTask(), cachedSinkName + "_pulsar_sink_sender-" + i);
            sinkThreadPool[i].start();
        }
        // set mq cluster ready
        setMQClusterStarted();
        logger.info("{} sink logic startted", cachedSinkName);
    }

    @Override
    public void stopSinkProcess() {
        logger.info("{} sink logic stopping...", cachedSinkName);
        int waitCount = 0;
        // Try to wait for the messages in the cache to be sent
        while (dispatchQueue.size() > 0 && waitCount++ < 10) {
            try {
                Thread.currentThread().sleep(800);
            } catch (InterruptedException e) {
                logger.info("{} stop thread has been interrupt!", cachedSinkName);
                break;
            }
        }
        // set send flag to false
        this.canSend = false;
        // shut down producers
        producerMap.clear();
        if (pulsarClient != null) {
            try {
                pulsarClient.shutdown();
            } catch (PulsarClientException e) {
                logger.error("{} destroy pulsarClient PulsarClientException", cachedSinkName, e);
            } catch (Exception e) {
                logger.error("{} destroy pulsarClient Exception", cachedSinkName, e);
            }
        }
        pulsarClient = null;
        // close message duplicate cache
        msgIdCache.clearMsgIdCache();
        logger.info("{} sink logic stopped", cachedSinkName);
    }

    @Override
    public void reloadMetaConfig() {
        Set<String> curTopicSet = ConfigManager.getInstance().getAllSinkPulsarXfeTopics();
        if (curTopicSet.isEmpty() || lastRefreshTopics.equals(curTopicSet)) {
            return;
        }
        boolean added = false;
        // for first reload, delay 3 seconds
        if (isFirstReload) {
            synchronized (lastRefreshTopics) {
                if (isFirstReload) {
                    isFirstReload = false;
                    long dltTime = System.currentTimeMillis() - startTime;
                    if (dltTime > 0 && dltTime < 3000) {
                        try {
                            Thread.sleep(dltTime);
                        } catch (Throwable e) {
                            //
                        }
                    }
                }
            }
        }
        List<String> addedTopics = new ArrayList<>();
        synchronized (producerMap) {
            for (String topic : curTopicSet) {
                if (topic == null) {
                    continue;
                }
                if (!lastRefreshTopics.contains(topic)) {
                    addedTopics.add(topic);
                    added = true;
                }
            }
            if (!added) {
                logger.info("{} topics changed, no added topics, reload topics are {}, cached topics are {}",
                        cachedSinkName, curTopicSet, lastRefreshTopics);
                return;
            }
            // publish need added topics
            publishTopics(addedTopics);
            logger.info("{} topics changed, added topics {}, reload topics are {}, cached topics are {}",
                    cachedSinkName, addedTopics, curTopicSet, lastRefreshTopics);
        }
    }

    private void publishTopics(List<String> addedTopics) {
        if (addedTopics.isEmpty()) {
            return;
        }

        int addedCnt = 0;
        int reCreateCnt = 0;
        int createFailCnt = 0;
        Producer producer;
        for (String topic : addedTopics) {
            producer = producerMap.get(topic);
            if (producer != null && producer.isConnected()) {
                continue;
            }
            if (producer == null) {
                addedCnt++;
            } else {
                reCreateCnt++;
                producer.closeAsync();
                logger.warn("[{}] producer is not connected, producer will be recreate", topic);
            }
            try {
                producer = pulsarClient.newProducer()
                        .sendTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                        .topic(topic)
                        .enableBatching(enableProducerBatch)
                        .blockIfQueueFull(blockIfProducerQueueFull)
                        .maxPendingMessages(maxProducerPendingMsgs)
                        .batchingMaxMessages(maxProducerBatchingMsgs)
                        .create();
                producerMap.put(topic, producer);
                // update cached topics
                lastRefreshTopics.add(topic);
            } catch (Throwable e) {
                createFailCnt++;
                logger.error("Create topic {}'s producer failed!", topic, e);
            }
        }
        logger.info(
                "{} publishTopics {}, totalTopicCnt={}, addedProducer = {}, re-createProducer={}, create-fail={}, cost: {} ms",
                cachedSinkName, addedTopics, addedTopics.size(), addedCnt, reCreateCnt, createFailCnt,
                (System.currentTimeMillis() - startTime));
    }

    private class SinkTask implements Runnable {

        @Override
        public void run() {
            logger.info("task {} start send message logic.", Thread.currentThread().getName());
            EventProfile profile = null;
            while (canSend) {
                try {
                    // take event profile
                    profile = takeDispatchedRecord();
                    if (profile == null) {
                        continue;
                    }
                    // send message
                    sendMessage(profile);
                } catch (Throwable e1) {
                    if (profile != null) {
                        offerDispatchRecord(profile);
                    }
                    if (logCounter.shouldPrint()) {
                        logger.error("{} send message failure", Thread.currentThread().getName(), e1);
                    }
                    // sleep some time
                    try {
                        Thread.sleep(maxSendFailureWaitDurMs);
                    } catch (Throwable e2) {
                        //
                    }
                }
            }
            logger.info("TubeMQSink task {} exits send message logic.", Thread.currentThread().getName());
        }

        private boolean sendMessage(EventProfile profile) {
            // get topic name
            String topic = ConfigManager.getInstance().getPulsarXfeSinkTopic(
                    profile.getGroupId(), profile.getStreamId());
            if (topic == null) {
                if (!CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
                    fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_CONFIG_TOPIC_MISSING,
                            getGroupIdStreamIdKey(profile.getGroupId(), profile.getStreamId()));
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    return false;
                }
                topic = CommonConfigHolder.getInstance().getRandDefTopics();
                if (StringUtils.isEmpty(topic)) {
                    fileMetricIncSumStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_MISSING);
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    return false;
                }
                fileMetricIncSumStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_USED);
            }
            // get producer by topic
            Producer producer = producerMap.get(topic);
            if (producer == null) {
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_PRODUCER_NULL, topic);
                processSendFail(profile, DataProxyErrCode.PRODUCER_IS_NULL, "");
                return false;
            }
            // check duplicate
            String msgSeqId = profile.getProperties().get(ConfigConstants.SEQUENCE_ID);
            if (msgIdCache.cacheIfAbsent(msgSeqId)) {
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_MESSAGE_DUPLICATE, topic);
                if (logDupMsgPrinter.shouldPrint()) {
                    logger.info("{} package {} existed,just discard.", cachedSinkName, msgSeqId);
                }
                return false;
            }
            // add headers
            final String xfeTopic = topic;
            long sendTime = System.currentTimeMillis();
            Map<String, String> msgAttrs = profile.getPropsToMQ(sendTime);
            try {
                producer.newMessage()
                        .properties(msgAttrs)
                        .value(profile.getEventBody())
                        .eventTime(profile.getDt())
                        .sendAsync().thenAccept((msgId) -> {
                            handleSendSuccessResult(xfeTopic, (MessageIdImpl) msgId, profile);
                        }).exceptionally((e) -> {
                            handleSendExceptionResult(xfeTopic, e, profile);
                            return null;
                        });
                return true;
            } catch (Throwable ex) {
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_SEND_EXCEPTION, topic);
                processSendFail(profile, DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
                if (logCounter.shouldPrint()) {
                    logger.error("Send Message to Tube failure", ex);
                }
                return false;
            }
        }
    }

    public void handleSendSuccessResult(String xfeTopic, MessageIdImpl result, EventProfile profile) {
        fileMetricAddSuccStats(profile, xfeTopic, result.toString());
        releaseAcquiredSizePermit(profile);
        addSendResultMetric(profile);
        profile.ack();
    }

    public void handleSendExceptionResult(String xfeTopic, Object ex, EventProfile profile) {
        fileMetricAddExceptStats(profile, xfeTopic, "", xfeTopic);
        processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, ex.toString());
        if (logCounter.shouldPrint()) {
            logger.error("Send message to {}, exception = {}", xfeTopic, ex);
        }
    }

    /**
     * processSendFail
     */
    public void processSendFail(EventProfile profile, DataProxyErrCode errCode, String errMsg) {
        msgIdCache.invalidCache(profile.getProperties().get(ConfigConstants.SEQUENCE_ID));
        if (profile.isResend(enableRetryAfterFailure, maxMsgRetries)) {
            offerDispatchRecord(profile);
            fileMetricIncSumStats(StatConstants.EVENT_SINK_FAILRETRY);
        } else {
            releaseAcquiredSizePermit(profile);
            fileMetricIncSumStats(StatConstants.EVENT_SINK_FAILDROPPED);
            profile.fail(errCode, errMsg);
        }
    }

    private String getGroupIdStreamIdKey(String groupId, String streamId) {
        return groupId + "/" + streamId;
    }

}
