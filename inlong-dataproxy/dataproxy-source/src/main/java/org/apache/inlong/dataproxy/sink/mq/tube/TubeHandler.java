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

package org.apache.inlong.dataproxy.sink.mq.tube;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.sink.common.EventHandler;
import org.apache.inlong.dataproxy.sink.mq.BatchPackProfile;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueZoneSinkContext;
import org.apache.inlong.dataproxy.sink.mq.PackProfile;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;
import org.apache.inlong.dataproxy.utils.DateTimeUtils;
import org.apache.inlong.sdk.commons.protocol.EventConstants;

import com.google.common.base.Preconditions;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.exception.TubeClientException;
import com.tencent.tubemq.client.factory.TubeMultiSessionFactory;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.MessageSentCallback;
import com.tencent.tubemq.client.producer.MessageSentResult;
import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * TubeHandler
 */
public class TubeHandler implements MessageQueueHandler {

    private static final Logger logger = LoggerFactory.getLogger(TubeHandler.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 150000, 60 * 1000);
    private static final LogCounter logMsgOverSizePrinter = new LogCounter(10, 100000, 40 * 1000);

    private static String MASTER_HOST_PORT_LIST = "master-host-port-list";

    private CacheClusterConfig config;
    private String clusterName;
    private MessageQueueZoneSinkContext sinkContext;

    // parameter
    private String masterHostAndPortList;
    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    // rpc request timeout ms
    private int requestTimeoutMs;
    // tube producer
    private TubeMultiSessionFactory sessionFactory;
    private MessageProducer producer;
    private final Set<String> topicSet = new HashSet<>();
    private final ThreadLocal<EventHandler> handlerLocal = new ThreadLocal<>();

    /**
     * init
     * @param config   the cluster configure
     * @param sinkContext the sink context
     */
    @Override
    public void init(CacheClusterConfig config, MessageQueueZoneSinkContext sinkContext) {
        this.config = config;
        this.clusterName = config.getClusterName();
        this.sinkContext = sinkContext;
    }

    /**
     * start
     */
    @Override
    public void start() {
        logger.info("{}'s TubeMQ handler starting....", sinkContext.getSinkName());
        // create tube producer
        try {
            // prepare configuration
            TubeClientConfig conf = initTubeConfig();
            logger.info("{} try to create {}'s producer:{}",
                    sinkContext.getSinkName(), this.clusterName, conf.toJsonString());
            this.sessionFactory = new TubeMultiSessionFactory(conf);
            this.producer = sessionFactory.createProducer();
            logger.info("{} created {}'s new producer success",
                    sinkContext.getSinkName(), this.clusterName);
            logger.info("{}'s TubeMQ handler started", sinkContext.getSinkName());
        } catch (Throwable e) {
            logger.error("{} start TubeMQ handler exception", sinkContext.getSinkName(), e);
        }
    }

    @Override
    public void publishTopic(Set<String> newTopicSet) {
        if (this.producer == null || newTopicSet == null || newTopicSet.isEmpty()) {
            return;
        }
        Set<String> published;
        try {
            published = producer.publish(newTopicSet);
            this.topicSet.addAll(newTopicSet);
            logger.info("{} published topics to {}, need publish are {}, published are {}",
                    sinkContext.getSinkName(), this.clusterName, newTopicSet, published);
        } catch (Throwable e) {
            logger.warn("{} publish topics to {} throw failure",
                    sinkContext.getSinkName(), this.clusterName, e);
        }
    }

    /**
     * initTubeConfig
     * @return the client configure
     *
     * @throws Exception
     */
    private TubeClientConfig initTubeConfig() throws Exception {
        // get parameter
        Context context = sinkContext.getProducerContext();
        Context configContext = new Context(context.getParameters());
        configContext.putAll(this.config.getParams());
        masterHostAndPortList = configContext.getString(MASTER_HOST_PORT_LIST);

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

        // get rpc request timeout ms
        this.requestTimeoutMs = context.getInteger(
                ConfigConstants.CLIENT_REQUEST_TIMEOUT_MS, ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS);
        Preconditions.checkArgument((this.requestTimeoutMs >= ConfigConstants.VAL_MIN_REQUEST_TIMEOUT_MS),
                ConfigConstants.CLIENT_REQUEST_TIMEOUT_MS + " must be >= "
                        + ConfigConstants.VAL_MIN_REQUEST_TIMEOUT_MS);
        // config
        final TubeClientConfig tubeClientConfig = new TubeClientConfig(this.masterHostAndPortList);
        tubeClientConfig.setLinkMaxAllowedDelayedMsgCount(linkMaxAllowedDelayedMsgCount);
        tubeClientConfig.setSessionWarnDelayedMsgCount(sessionWarnDelayedMsgCount);
        tubeClientConfig.setSessionMaxAllowedDelayedMsgCount(sessionMaxAllowedDelayedMsgCount);
        tubeClientConfig.setNettyWriteBufferHighWaterMark(nettyWriteBufferHighWaterMark);
        tubeClientConfig.setHeartbeatPeriodMs(15000L);
        tubeClientConfig.setRpcTimeoutMs(this.requestTimeoutMs);

        return tubeClientConfig;
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        logger.info("{}'s TubeMQ handler stopping.....", sinkContext.getSinkName());
        // producer
        if (this.producer != null) {
            try {
                this.producer.shutdown();
            } catch (Throwable e) {
                logger.error("{}'s TubeMQ handler shutdown producer failure",
                        sinkContext.getSinkName(), e);
            }
        }
        if (this.sessionFactory != null) {
            try {
                this.sessionFactory.shutdown();
            } catch (TubeClientException e) {
                logger.error("{}'s TubeMQ handler shutdown sessionFactory failure", sinkContext.getSinkName(), e);
            }
        }
        logger.info("{}'s TubeMQ handler stopped", sinkContext.getSinkName());
    }

    /**
     * send
     */
    public boolean send(PackProfile profile) {
        String topic = null;
        try {
            // idConfig
            IdTopicConfig idConfig = ConfigManager.getInstance().getSinkIdTopicConfig(
                    profile.getInlongGroupId(), profile.getInlongStreamId());
            if (idConfig == null) {
                // add default topics first
                if (CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
                    topic = CommonConfigHolder.getInstance().getRandDefTopics();
                    if (StringUtils.isEmpty(topic)) {
                        sinkContext.fileMetricIncWithDetailStats(
                                StatConstants.EVENT_SINK_DEFAULT_TOPIC_MISSING, profile.getUid());
                        sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                        sinkContext.getMqZoneSink().releaseAcquiredSizePermit(profile);
                        profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                        return false;
                    }
                    sinkContext.fileMetricIncWithDetailStats(
                            StatConstants.EVENT_SINK_DEFAULT_TOPIC_USED, profile.getUid());
                } else {
                    sinkContext.fileMetricIncWithDetailStats(
                            StatConstants.EVENT_SINK_CONFIG_TOPIC_MISSING, profile.getUid());
                    sinkContext.addSendResultMetric(profile, clusterName, profile.getUid(), false, 0);
                    sinkContext.getMqZoneSink().releaseAcquiredSizePermit(profile);
                    profile.fail(DataProxyErrCode.GROUPID_OR_STREAMID_NOT_CONFIGURE, "");
                    return false;
                }
            } else {
                topic = idConfig.getTopicName();
            }
            // create producer failed
            if (producer == null) {
                sinkContext.fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_PRODUCER_NULL, topic);
                sinkContext.processSendFail(profile, clusterName, topic, 0,
                        DataProxyErrCode.PRODUCER_IS_NULL, "");
                return false;
            }
            // publish
            if (!this.topicSet.contains(topic)) {
                this.producer.publish(topic);
                this.topicSet.add(topic);
            }
            // send
            if (profile instanceof SimplePackProfile) {
                this.sendSimplePackProfile((SimplePackProfile) profile, idConfig, topic);
            } else {
                this.sendBatchPackProfile((BatchPackProfile) profile, idConfig, topic);
            }
            return true;
        } catch (Throwable ex) {
            sinkContext.fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_SEND_EXCEPTION, topic);
            sinkContext.processSendFail(profile, clusterName, profile.getUid(), 0,
                    DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
            if (logCounter.shouldPrint()) {
                logger.error("{} send message to TubeMQ failure", sinkContext.getSinkName(), ex);
            }
            return false;
        }
    }

    /**
     * send BatchPackProfile
     */
    private void sendBatchPackProfile(BatchPackProfile batchProfile, IdTopicConfig idConfig,
            String topic) throws Exception {
        EventHandler handler = handlerLocal.get();
        if (handler == null) {
            handler = this.sinkContext.createEventHandler();
            handlerLocal.set(handler);
        }
        // get headers to mq
        Map<String, String> headers = handler.parseHeader(idConfig, batchProfile, sinkContext.getNodeId(),
                sinkContext.getCompressType());
        // compress
        byte[] bodyBytes = handler.parseBody(idConfig, batchProfile, sinkContext.getCompressType());
        Message message = new Message(topic, bodyBytes);
        // add headers
        long dataTimeL = Long.parseLong(headers.get(EventConstants.HEADER_KEY_PACK_TIME));
        message.putSystemHeader(batchProfile.getInlongStreamId(), DateTimeUtils.ms2yyyyMMddHHmm(dataTimeL));
        headers.forEach(message::setAttrKeyVal);
        // metric
        sinkContext.addSendMetric(batchProfile, clusterName, topic, bodyBytes.length);
        // callback
        long sendTime = System.currentTimeMillis();
        MessageSentCallback callback = new MessageSentCallback() {

            @Override
            public void onMessageSent(MessageSentResult result) {
                if (result.isSuccess()) {
                    sinkContext.fileMetricIncSumStats(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(batchProfile, clusterName, topic, true, sendTime);
                    sinkContext.getMqZoneSink().releaseAcquiredSizePermit(batchProfile);
                    batchProfile.ack();
                } else {
                    sinkContext.fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_FAILURE,
                            topic + "." + result.getErrCode());
                    sinkContext.processSendFail(batchProfile, clusterName, topic, sendTime,
                            DataProxyErrCode.MQ_RETURN_ERROR, result.getErrMsg());
                    if (result.getErrCode() == TErrCodeConstants.PARAMETER_MSG_OVER_MAX_LENGTH) {
                        if (logMsgOverSizePrinter.shouldPrint()) {
                            logger.error("OVER-MAX-ERROR: Topic ({}) over max-length",
                                    topic, result.getErrMsg());
                        }
                    } else {
                        if (logCounter.shouldPrint()) {
                            logger.warn("{} send message to tube {} failure: {}",
                                    sinkContext.getSinkName(), topic, result.getErrMsg());
                        }
                    }
                }
            }

            @Override
            public void onException(Throwable ex) {
                sinkContext.fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_RECEIVEEXCEPT, topic);
                sinkContext.processSendFail(batchProfile, clusterName, topic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                if (logCounter.shouldPrint()) {
                    logger.warn("{} send ProfileV1 to TubeMQ's {} exception",
                            sinkContext.getSinkName(), topic, ex);
                }
            }
        };
        producer.sendMessage(message, callback);
    }

    /**
     * sendSimpleProfileV0
     */
    private void sendSimplePackProfile(SimplePackProfile simpleProfile, IdTopicConfig idConfig,
            String topic) throws Exception {
        // metric
        sinkContext.addSendMetric(simpleProfile, clusterName,
                topic, simpleProfile.getEvent().getBody().length);
        // build message
        Message message = new Message(topic, simpleProfile.getEvent().getBody());
        long dataTimeL = Long.parseLong(simpleProfile.getProperties().get(ConfigConstants.PKG_TIME_KEY));
        message.putSystemHeader(simpleProfile.getInlongStreamId(), DateTimeUtils.ms2yyyyMMddHHmm(dataTimeL));
        // add headers
        long sendTime = System.currentTimeMillis();
        Map<String, String> headers = simpleProfile.getPropsToMQ(sendTime);
        headers.forEach(message::setAttrKeyVal);
        // callback
        MessageSentCallback callback = new MessageSentCallback() {

            @Override
            public void onMessageSent(MessageSentResult result) {
                if (result.isSuccess()) {
                    sinkContext.fileMetricAddSuccStats(simpleProfile, topic, result.getPartition().getHost());
                    sinkContext.addSendResultMetric(simpleProfile, clusterName, topic, true, sendTime);
                    sinkContext.getMqZoneSink().releaseAcquiredSizePermit(simpleProfile);
                    simpleProfile.ack();
                } else {
                    String brokerIP = "";
                    if (result.getPartition() != null) {
                        brokerIP = result.getPartition().getHost();
                    }
                    sinkContext.fileMetricAddFailStats(simpleProfile, topic,
                            brokerIP, topic + "." + result.getErrCode());
                    sinkContext.processSendFail(simpleProfile, clusterName, topic, sendTime,
                            DataProxyErrCode.MQ_RETURN_ERROR, result.getErrMsg());
                    if (result.getErrCode() == TErrCodeConstants.PARAMETER_MSG_OVER_MAX_LENGTH) {
                        if (logMsgOverSizePrinter.shouldPrint()) {
                            logger.error("OVER-MAX-ERROR: Topic ({}) over max-length",
                                    topic, result.getErrMsg());
                        }
                    } else {
                        if (logCounter.shouldPrint()) {
                            logger.warn("{} send message to tube {} failure: {}",
                                    sinkContext.getSinkName(), topic, result.getErrMsg());
                        }
                    }
                }
            }

            @Override
            public void onException(Throwable ex) {
                sinkContext.fileMetricAddExceptStats(simpleProfile, topic, "", topic);
                sinkContext.processSendFail(simpleProfile, clusterName, topic, sendTime,
                        DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
                if (logCounter.shouldPrint()) {
                    logger.warn("{} send Message to TubeMQ's {} exception",
                            sinkContext.getSinkName(), topic, ex);
                }
            }
        };
        producer.sendMessage(message, callback);
    }
}
