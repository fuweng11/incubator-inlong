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
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.factory.TubeMultiSessionFactory;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.MessageSentCallback;
import com.tencent.tubemq.client.producer.MessageSentResult;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TubeMQSink extends BaseSink {

    private static final Logger logger = LoggerFactory.getLogger(TubeMQSink.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);
    private static final LogCounter logDupMsgPrinter = new LogCounter(10, 100000, 30 * 1000);
    private static final LogCounter logMsgOverSizePrinter = new LogCounter(10, 100000, 30 * 1000);

    private static final String MAX_TOPICS_EACH_PRODUCER_HOLD_NAME = "max-topic-each-producer-hold";
    private static final int VAL_DEF_TOPICS_EACH_PRODUCER_HOLD_NAME = 200;
    private static final int VAL_MIN_TOPICS_EACH_PRODUCER_HOLD_NAME = 1;

    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    private long sendStatusCheckMsgCnt;
    private long sendMsgFailureCheckCnt;
    private long sendThrownCheckCnt;
    private long sendSlowSleepMs;

    // last refresh topics
    private final Set<String> lastRefreshTopics = new HashSet<>();
    // session factory
    private TubeMultiSessionFactory sessionFactory;
    // topic-producer map
    private static final ConcurrentHashMap<String, MessageProducer> producerMap = new ConcurrentHashMap<>();
    // latest producer
    private MessageProducer latestProducer;
    private int maxAllowedPublishTopicNum;
    private final AtomicInteger latestPublishTopicNum = new AtomicInteger(0);
    // whether to send message
    private volatile boolean canSend = true;

    @Override
    public void configure(Context context) {
        super.configure(context);

        this.clusterAddrList = context.getString(ConfigConstants.MASTER_SERVER_URL_LIST);
        Preconditions.checkState(clusterAddrList != null,
                ConfigConstants.MASTER_SERVER_URL_LIST + " parameter not specified");

        this.linkMaxAllowedDelayedMsgCount = context.getLong(
                ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT, ConfigConstants.VAL_DEF_ALLOWED_DELAYED_MSG_COUNT);
        Preconditions.checkArgument(
                (this.linkMaxAllowedDelayedMsgCount >= ConfigConstants.VAL_MIN_ALLOWED_DELAYED_MSG_COUNT),
                ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT + " must be >= "
                        + ConfigConstants.VAL_MIN_ALLOWED_DELAYED_MSG_COUNT);

        this.sessionWarnDelayedMsgCount = context.getLong(
                ConfigConstants.SESSION_WARN_DELAYED_MSG_COUNT, ConfigConstants.VAL_DEF_SESSION_WARN_DELAYED_MSG_COUNT);
        Preconditions.checkArgument(
                (this.sessionWarnDelayedMsgCount >= ConfigConstants.VAL_MIN_SESSION_WARN_DELAYED_MSG_COUNT),
                ConfigConstants.SESSION_WARN_DELAYED_MSG_COUNT + " must be >= "
                        + ConfigConstants.VAL_MIN_SESSION_WARN_DELAYED_MSG_COUNT);

        this.sessionMaxAllowedDelayedMsgCount = context.getLong(
                ConfigConstants.SESSION_MAX_ALLOWED_DELAYED_MSG_COUNT,
                ConfigConstants.VAL_DEF_SESSION_DELAYED_MSG_COUNT);
        Preconditions.checkArgument(
                (this.sessionMaxAllowedDelayedMsgCount >= ConfigConstants.VAL_MIN_SESSION_DELAYED_MSG_COUNT),
                ConfigConstants.SESSION_MAX_ALLOWED_DELAYED_MSG_COUNT + " must be >= "
                        + ConfigConstants.VAL_MIN_SESSION_DELAYED_MSG_COUNT);

        this.nettyWriteBufferHighWaterMark = context.getLong(
                ConfigConstants.NETTY_WRITE_BUFFER_HIGH_WATER_MARK,
                ConfigConstants.VAL_DEF_NETTY_WRITE_HIGH_WATER_MARK);
        Preconditions.checkArgument(
                (this.nettyWriteBufferHighWaterMark >= ConfigConstants.VAL_MIN_NETTY_WRITE_HIGH_WATER_MARK),
                ConfigConstants.NETTY_WRITE_BUFFER_HIGH_WATER_MARK + " must be >= "
                        + ConfigConstants.VAL_MIN_NETTY_WRITE_HIGH_WATER_MARK);

        this.maxAllowedPublishTopicNum = context.getInteger(
                MAX_TOPICS_EACH_PRODUCER_HOLD_NAME, VAL_DEF_TOPICS_EACH_PRODUCER_HOLD_NAME);
        Preconditions.checkArgument((this.maxAllowedPublishTopicNum >= VAL_MIN_TOPICS_EACH_PRODUCER_HOLD_NAME),
                MAX_TOPICS_EACH_PRODUCER_HOLD_NAME + " must be >= " + VAL_MIN_TOPICS_EACH_PRODUCER_HOLD_NAME);

        this.sendStatusCheckMsgCnt = context.getLong(
                ConfigConstants.SEND_WORKER_STATUS_CHECK_MSG_CNT,
                ConfigConstants.VAL_DEF_SEND_WORKER_STATUS_MSG_CNT);

        this.sendMsgFailureCheckCnt = context.getLong(
                ConfigConstants.SEND_WORKER_MSG_FAILURE_CHECK_CNT,
                ConfigConstants.VAL_DEF_SEND_WORKER_MSG_FAILURE_CHECK_CNT);
        Preconditions.checkArgument(
                (this.sendMsgFailureCheckCnt >= ConfigConstants.VAL_MIN_SEND_WORKER_MSG_FAILURE_CHECK_CNT),
                ConfigConstants.SEND_WORKER_MSG_FAILURE_CHECK_CNT + " must be >= "
                        + ConfigConstants.VAL_MIN_SEND_WORKER_MSG_FAILURE_CHECK_CNT);

        this.sendThrownCheckCnt = context.getLong(
                ConfigConstants.SEND_WORKER_THROWN_CHECK_MSG_CNT,
                ConfigConstants.VAL_DEF_SEND_WORKER_THROWN_MSG_CNT);

        this.sendSlowSleepMs = context.getLong(
                ConfigConstants.SEND_WORKER_SLOW_SLEEP_MS,
                ConfigConstants.VAL_DEF_SEND_WORKER_SLOW_SLEEP_MS);
        Preconditions.checkArgument(
                (this.sendSlowSleepMs >= ConfigConstants.VAL_MIN_SEND_WORKER_SLOW_SLEEP_MS),
                ConfigConstants.SEND_WORKER_SLOW_SLEEP_MS + " must be >= "
                        + ConfigConstants.VAL_MIN_SEND_WORKER_SLOW_SLEEP_MS);
    }

    @Override
    public void startSinkProcess() {
        logger.info("{} sink logic starting...", cachedSinkName);
        // register meta-configure listener to config-manager
        ConfigManager.getInstance().regTDBankMetaChgCallback(this);
        try {
            TubeClientConfig conf = initTubeConfig();
            sessionFactory = new TubeMultiSessionFactory(conf);
        } catch (Throwable e) {
            stop();
            logger.error("{} create session factory failure, please re-check. ex2 {}",
                    cachedSinkName, e.getMessage());
            throw new FlumeException("create session factory failure, "
                    + "maybe tube master set error/shutdown in progress, please re-check");
        }
        // start message process logic
        for (int i = 0; i < sinkThreadPool.length; i++) {
            sinkThreadPool[i] = new Thread(new SinkTask(this), cachedSinkName + "_tube_sink_sender-" + i);
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
        for (Map.Entry<String, MessageProducer> entry : producerMap.entrySet()) {
            if (entry == null || entry.getValue() == null) {
                continue;
            }
            try {
                entry.getValue().shutdown();
            } catch (Throwable e) {
                logger.error("{} destroy producer error: {}", cachedSinkName, e.getMessage());
            }
        }
        // shutdown session factory
        if (sessionFactory != null) {
            try {
                sessionFactory.shutdown();
            } catch (Throwable e) {
                logger.error("{} destroy session factory error: {}", cachedSinkName, e.getMessage());
            }
            sessionFactory = null;
        }
        // close message duplicate cache
        msgIdCache.clearMsgIdCache();
        logger.info("{} sink logic stopped", cachedSinkName);
    }

    @Override
    public void reloadMetaConfig() {
        Set<String> curTopicSet = ConfigManager.getInstance().getAllSinkTDBankTopicNames();
        if (curTopicSet.isEmpty() || lastRefreshTopics.equals(curTopicSet)) {
            return;
        }
        boolean added = false;
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
            // update cached topics
            lastRefreshTopics.addAll(curTopicSet);
            if (!added) {
                logger.info("{} topics changed, but no added topics", cachedSinkName);
                return;
            }
            // publish need added topics
            publishTopics(addedTopics);
        }
    }

    private void publishTopics(List<String> addedTopics) {
        if (addedTopics.isEmpty()) {
            return;
        }
        int remainder;
        int startIndex;
        int endIndex = 0;
        Set<String> successSet = new HashSet<>();
        Set<String> subSet = new HashSet<>();
        long startTime = System.currentTimeMillis();
        Collections.sort(addedTopics);
        do {
            subSet.clear();
            if (isShutdown) {
                break;
            }
            remainder = maxAllowedPublishTopicNum - latestPublishTopicNum.get();
            if (remainder == 0 || latestProducer == null) {
                try {
                    latestProducer = sessionFactory.createProducer();
                } catch (Throwable e1) {
                    fileMetricIncSumStats(StatConstants.EVENT_SINK_PRODUCER_CREATE_FAILURE);
                    logger.warn("{} create producer failure", cachedSinkName, e1);
                    continue;
                }
                latestPublishTopicNum.set(0);
                remainder = maxAllowedPublishTopicNum - latestPublishTopicNum.get();
            }
            startIndex = endIndex;
            endIndex = Math.min(startIndex + remainder, addedTopics.size());
            subSet.addAll(addedTopics.subList(startIndex, endIndex));
            try {
                successSet.addAll(latestProducer.publish(subSet));
            } catch (Throwable e) {
                if (logCounter.shouldPrint()) {
                    logger.warn("{} publish topics failure, topics = {}", cachedSinkName, subSet, e);
                }
            }
            for (String topic : subSet) {
                producerMap.put(topic, latestProducer);
            }
            latestPublishTopicNum.addAndGet(subSet.size());
        } while (endIndex < addedTopics.size());
        if (successSet.size() < addedTopics.size()) {
            Set<String> failSet = new HashSet<>();
            for (String topic : addedTopics) {
                if (successSet.contains(topic)) {
                    continue;
                }
                failSet.add(topic);
            }
            logger.warn("{} failure to published topic set {}, cost: {} ms",
                    cachedSinkName, failSet, (System.currentTimeMillis() - startTime));
        } else {
            logger.info("{} published topics, added topics {}, cost: {} ms",
                    cachedSinkName, addedTopics, (System.currentTimeMillis() - startTime));
        }
    }

    private class SinkTask implements Runnable {

        private long sendThrownCnt = 0L;
        private final AtomicLong cumExceptionCnt = new AtomicLong(0);
        private final long sendStatusCheckMsgCnt;
        private final long sendMsgFailureCheckCnt;
        private final long sendThrownCheckCnt;
        private final long sendSlowSleepMs;
        private final long sendHighThrownCheckCnt;
        private final long sendHighSlowSleepMs;

        public SinkTask(TubeMQSink tubeMQSink) {
            this.sendStatusCheckMsgCnt = tubeMQSink.sendStatusCheckMsgCnt;
            this.sendMsgFailureCheckCnt = tubeMQSink.sendMsgFailureCheckCnt;
            this.sendThrownCheckCnt = tubeMQSink.sendThrownCheckCnt;
            this.sendSlowSleepMs = tubeMQSink.sendSlowSleepMs;
            this.sendHighThrownCheckCnt = tubeMQSink.sendThrownCheckCnt * 2;
            this.sendHighSlowSleepMs = tubeMQSink.sendSlowSleepMs * 2;
        }

        @Override
        public void run() {
            logger.info("task {} start send message logic.", Thread.currentThread().getName());
            long sentCnt = 0;
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
                if ((this.sendStatusCheckMsgCnt > 0)
                        && (++sentCnt % this.sendStatusCheckMsgCnt == 0)) {
                    if (this.cumExceptionCnt.get() >= this.sendMsgFailureCheckCnt) {
                        fileMetricIncSumStats(StatConstants.EVENT_SINK_SEND_WORKER_SLEEP_COUNT);
                        try {
                            Thread.sleep(this.sendSlowSleepMs);
                        } catch (Throwable ex2) {
                            //
                        }
                    }
                }
            }
            logger.info("TubeMQSink task {} exits send message logic.", Thread.currentThread().getName());
        }

        private boolean sendMessage(EventProfile profile) {
            // check task whether invalid
            if (profile.isInValidTask()) {
                releaseAcquiredSizePermit(profile.getMsgSize());
                fileMetricIncWithDetailStats(
                        StatConstants.EVENT_SINK_CHANNEL_INVALID_DROPPED, profile.getGroupId());
                profile.clear();
                return false;
            }
            // parse fields in headers
            profile.parseFields();
            // get topic name
            String topic = profile.getSrcTopic();
            MessageProducer producer = producerMap.get(topic);
            if (producer == null) {
                if (CommonConfigHolder.getInstance().isDefTopic(topic)) {
                    fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_DEF_TOPIC_WITHOUT_PRODUCER, topic);
                    processSendFail(profile, DataProxyErrCode.PRODUCER_IS_NULL, "");
                    return false;
                }
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_TOPIC_WITHOUT_PRODUCER, topic);
                // add default topics first
                topic = CommonConfigHolder.getInstance().getRandDefTopics();
                if (StringUtils.isEmpty(topic)) {
                    releaseAcquiredSizePermit(profile.getMsgSize());
                    fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_MISSING,
                            profile.getGroupId());
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    profile.clear();
                    return false;
                }
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_DEFAULT_TOPIC_USED, profile.getGroupId());
                // get producer by topic again
                producer = producerMap.get(topic);
                if (producer == null) {
                    fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_DEF_TOPIC_WITHOUT_PRODUCER, topic);
                    processSendFail(profile, DataProxyErrCode.PRODUCER_IS_NULL, "");
                    return false;
                }
            }
            // check duplicate
            if (msgIdCache.cacheIfAbsent(profile.getMsgSeqId())) {
                releaseAcquiredSizePermit(profile.getMsgSize());
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_MESSAGE_DUPLICATE, topic);
                if (logDupMsgPrinter.shouldPrint()) {
                    logger.info("{} package {} existed,just discard.", cachedSinkName, profile.getMsgSeqId());
                }
                profile.clear();
                return false;
            }
            long sendTime;
            Message message;
            try {
                if (profile.isBringEvent()) {
                    // build message
                    message = new Message(topic, profile.getEventBody());
                    sendTime = profile.setPropsToMQ(message);
                    EventProfile newProfile = new EventProfile(profile, message);
                    producer.sendMessage(message, new MyCallback(cumExceptionCnt, newProfile, sendTime, topic));
                    profile.clear();
                } else {
                    sendTime = profile.updateSendTime();
                    message = profile.getMessage();
                    producer.sendMessage(message, new MyCallback(cumExceptionCnt, profile, sendTime, topic));
                }
                sendThrownCnt = 0;
                return true;
            } catch (Throwable ex) {
                this.cumExceptionCnt.incrementAndGet();
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_SEND_EXCEPTION, topic);
                processSendFail(profile, DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
                if ((this.sendThrownCheckCnt > 0)
                        && (++sendThrownCnt > this.sendThrownCheckCnt)) {
                    fileMetricIncSumStats(StatConstants.EVENT_SINK_SEND_WORKER_SLEEP_COUNT);
                    try {
                        Thread.sleep((sendThrownCnt > this.sendHighThrownCheckCnt)
                                ? this.sendHighSlowSleepMs
                                : this.sendSlowSleepMs);
                    } catch (Throwable ex2) {
                        //
                    }
                }
                if (logCounter.shouldPrint()) {
                    logger.error("Send Message to Tube failure", ex);
                }
                return false;
            }
        }
    }

    private class MyCallback implements MessageSentCallback {

        private final AtomicLong cumFailureCnt;
        private final EventProfile profile;
        private final long sendTime;
        private final String topic;

        public MyCallback(AtomicLong cumFailureCnt, EventProfile profile, long sendTime, String topic) {
            this.cumFailureCnt = cumFailureCnt;
            this.profile = profile;
            this.sendTime = sendTime;
            this.topic = topic;
        }

        @Override
        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                this.cumFailureCnt.set(0);
                fileMetricAddSuccStats(profile, topic, result.getPartition().getHost());
                releaseAcquiredSizePermit(profile.getMsgSize());
                addSendResultMetric(profile);
                profile.ack();
                profile.clear();
            } else {
                fileMetricAddFailStats(profile, topic,
                        result.getPartition().getHost(), topic + "." + result.getErrCode());
                processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, result.getErrMsg());
                if (result.getErrCode() == TErrCodeConstants.PARAMETER_MSG_OVER_MAX_LENGTH) {
                    this.cumFailureCnt.incrementAndGet();
                    if (logMsgOverSizePrinter.shouldPrint()) {
                        logger.error("Message over max-length {}", topic, result.getErrMsg());
                    }
                } else {
                    if (logCounter.shouldPrint()) {
                        logger.error("Send message to tube failure: {}", result.getErrMsg());
                    }
                }
            }
        }

        @Override
        public void onException(Throwable ex) {
            fileMetricAddExceptStats(profile, topic, "", topic);
            processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
            if (logCounter.shouldPrint()) {
                logger.error("Send message to {} tube exception", topic, ex);
            }
        }
    }

    /**
     * processSendFail
     */
    public void processSendFail(EventProfile profile, DataProxyErrCode errCode, String errMsg) {
        msgIdCache.invalidCache(profile.getMsgSeqId());
        if (profile.isResend(enableRetryAfterFailure, maxRetries)) {
            offerDispatchRecord(profile);
            fileMetricIncSumStats(StatConstants.EVENT_SINK_FAILRETRY);
        } else {
            releaseAcquiredSizePermit(profile.getMsgSize());
            fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_FAILDROPPED, profile.getGroupId());
            profile.fail(errCode, errMsg);
            profile.clear();
        }
    }

    /**
     * Initial tubemq client configure
     * @return the client configure
     *
     */
    private TubeClientConfig initTubeConfig() {
        // config
        final TubeClientConfig tubeClientConfig = new TubeClientConfig(this.clusterAddrList);
        tubeClientConfig.setLinkMaxAllowedDelayedMsgCount(linkMaxAllowedDelayedMsgCount);
        tubeClientConfig.setSessionWarnDelayedMsgCount(sessionWarnDelayedMsgCount);
        tubeClientConfig.setSessionMaxAllowedDelayedMsgCount(sessionMaxAllowedDelayedMsgCount);
        tubeClientConfig.setNettyWriteBufferHighWaterMark(nettyWriteBufferHighWaterMark);
        tubeClientConfig.setHeartbeatPeriodMs(15000L);
        tubeClientConfig.setRpcTimeoutMs(requestTimeoutMs);
        return tubeClientConfig;
    }
}
