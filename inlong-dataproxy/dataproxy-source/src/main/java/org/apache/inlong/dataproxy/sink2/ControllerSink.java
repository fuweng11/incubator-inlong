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
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.dataproxy.base.SinkRspEvent;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.dataproxy.metrics.stats.MonitorIndex;
import org.apache.inlong.dataproxy.metrics.stats.MonitorStats;
import org.apache.inlong.dataproxy.metrics.stats.MonitorSumIndex;
import org.apache.inlong.dataproxy.utils.BufferQueue;
import org.apache.inlong.dataproxy.utils.DateTimeUtils;
import org.apache.inlong.sdk.commons.protocol.EventConstants;

import com.google.common.base.Preconditions;
import com.tencent.tdbank.ac.controller.ControllerException;
import com.tencent.tdbank.ac.controller.avro.FileStatus;
import com.tencent.tdbank.ac.controller.avro.FileStatusUtil;
import com.tencent.tdbank.ac.controller.client.ControllerRpcClient;
import com.tencent.tdbank.ac.controller.client.ControllerRpcClientFactory;
import com.tencent.tubemq.client.config.TubeClientConfig;
import com.tencent.tubemq.client.factory.TubeMultiSessionFactory;
import com.tencent.tubemq.client.producer.MessageProducer;
import com.tencent.tubemq.client.producer.MessageSentCallback;
import com.tencent.tubemq.client.producer.MessageSentResult;
import com.tencent.tubemq.corebase.Message;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ControllerSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ControllerSink.class);
    // log print count
    private static final LogCounter logAgentCounter = new LogCounter(10, 100000, 40 * 1000);
    private static final LogCounter logSendMsgCounter = new LogCounter(10, 100000, 40 * 1000);
    private static final LogCounter logPubTopicPrinter = new LogCounter(10, 100000, 40 * 1000);
    private static final LogCounter logDupMsgPrinter = new LogCounter(10, 100000, 40 * 1000);

    private static final String SEND_REMOTE = "send-remote";
    private static final boolean VAL_DEF_SEND_REMOTE = true;

    private static String CONTROLLER_CONFIG_FILE = "controller-config-file";
    private static final String MAX_RETRY_CNT_STR = "max-retry-cnt";

    private static final String AGENT_INDEX_TOPIC = "agent-index-topic";
    private static final String VAL_DEF_AGENT_INDEX_TOPIC = "teg_tdbank";

    private static final String INDEX_STATUS_TID = "agent-index-status-tid";
    private static final String VAL_DEF_INDEX_STATUS_TID = "agent_file_status_log";

    private static final String INDEX_MEASURE_TID = "agent-index-measure-tid";
    private static final String VAL_DEF_INDEX_MEASURE_TID = "agent_measure_log";

    // cached sink's name
    protected String cachedSinkName;
    // cached sink's channel
    protected Channel cachedMsgChannel;
    // whether the sink has closed
    protected volatile boolean isShutdown = false;
    // whether mq cluster connected
    protected volatile boolean mqClusterStarted = false;
    // message duplicate cache
    private boolean enableDeDupCheck;
    private int visitConcurLevel = 32;
    private int initCacheCapacity = 5000000;
    private long expiredDurSec = 30;
    protected MsgIdCache msgIdCache;
    // wether send message to mq
    private boolean sendRemote;
    // message send thread count
    protected int maxThreads;
    // message send thread pool
    protected Thread[] sinkThreadPool;
    // max inflight buffer size in KB
    private int maxInflightBufferSIzeInKB;
    // event dispatch queue
    private BufferQueue<EventProfile> dispatchQueue;
    // whether to send message
    private volatile boolean canSend = true;
    // controller agent
    private ControllerRpcClient controllerAgent;
    private String controllerConfigFile;
    private Properties controllerProp;
    // clusterMasterAddress
    protected String clusterAddrList;
    private TubeMultiSessionFactory sessionFactory = null;
    private MessageProducer messageProducer = null;
    private String agentMeasureTid;
    private String agentStatusTid;
    private String agentIndexTopic;
    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    // rpc request timeout ms
    protected int requestTimeoutMs;
    // whether to resend the message after sending failure
    protected boolean enableRetryAfterFailure;
    // max retry times if send failure
    protected int maxMsgRetries;
    // max wait time if send failure
    protected long maxSendFailureWaitDurMs;
    // mq cluster status wait duration
    private final long MQ_CLUSTER_STATUS_WAIT_DUR_MS = 2000L;
    // file metric statistic
    private boolean enableFileMetric;
    private MonitorStats monitorStats = null;
    private MonitorIndex detailIndex = null;
    private MonitorSumIndex sumIndex = null;
    // message encode type id
    private final String msgEncodeTypeId =
            MessageWrapType.TDMSG1.getStrId();

    public ControllerSink() {

    }

    @Override
    public void configure(Context context) {
        this.cachedSinkName = getName();
        logger.info("{} start to configure, context:{}.", this.cachedSinkName, context.toString());
        this.enableFileMetric = CommonConfigHolder.getInstance().isEnableFileMetric();
        // initial send mode
        this.sendRemote = context.getBoolean(SEND_REMOTE, VAL_DEF_SEND_REMOTE);
        // wether retry send if sent failure
        this.enableRetryAfterFailure =
                CommonConfigHolder.getInstance().isEnableSendRetryAfterFailure();
        // initial send retry count
        this.maxMsgRetries = context.getInteger(MAX_RETRY_CNT_STR,
                CommonConfigHolder.getInstance().getMaxRetriesAfterFailure());
        // get the number of sink worker thread
        this.maxThreads = context.getInteger(ConfigConstants.MAX_THREADS, ConfigConstants.VAL_DEF_SINK_THREADS);
        Preconditions.checkArgument((this.maxThreads >= ConfigConstants.VAL_MIN_SINK_THREADS),
                ConfigConstants.MAX_THREADS + " must be >= " + ConfigConstants.VAL_MIN_SINK_THREADS);
        // get message deduplicate setting
        this.enableDeDupCheck = context.getBoolean(ConfigConstants.ENABLE_MSG_CACHE_DEDUP,
                ConfigConstants.VAL_DEF_ENABLE_MSG_CACHE_DEDUP);
        this.maxInflightBufferSIzeInKB = context.getInteger(ConfigConstants.MAX_INFLIGHT_BUFFER_QUEUE_SIZE_KB,
                CommonConfigHolder.getInstance().getMaxBufferQueueSizeKb());
        Preconditions.checkArgument(
                (this.maxInflightBufferSIzeInKB >= ConfigConstants.VAL_MIN_INFLIGHT_BUFFER_QUEUE_SIZE_KB),
                ConfigConstants.MAX_INFLIGHT_BUFFER_QUEUE_SIZE_KB + " must be >= "
                        + ConfigConstants.VAL_MIN_INFLIGHT_BUFFER_QUEUE_SIZE_KB);
        this.maxSendFailureWaitDurMs = context.getLong(ConfigConstants.MAX_SEND_FAILURE_WAIT_DUR_MS,
                ConfigConstants.VAL_DEF_SEND_FAILURE_WAIT_DUR_MS);
        Preconditions.checkArgument(
                (this.maxSendFailureWaitDurMs >= ConfigConstants.VAL_MIN_SEND_FAILURE_WAIT_DUR_MS),
                ConfigConstants.MAX_SEND_FAILURE_WAIT_DUR_MS + " must be >= "
                        + ConfigConstants.VAL_MIN_SEND_FAILURE_WAIT_DUR_MS);

        if (this.sendRemote) {
            // initial sink worker thread pool
            this.sinkThreadPool = new Thread[this.maxThreads];
            this.clusterAddrList = context.getString(ConfigConstants.MASTER_SERVER_URL_LIST);
            Preconditions.checkState(this.clusterAddrList != null,
                    ConfigConstants.MASTER_SERVER_URL_LIST + " parameter not specified");
            this.agentIndexTopic = context.getString(AGENT_INDEX_TOPIC, VAL_DEF_AGENT_INDEX_TOPIC);
            this.agentStatusTid = context.getString(INDEX_STATUS_TID, VAL_DEF_INDEX_STATUS_TID);
            this.agentMeasureTid = context.getString(INDEX_MEASURE_TID, VAL_DEF_INDEX_MEASURE_TID);
            this.linkMaxAllowedDelayedMsgCount = context.getLong(
                    ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT,
                    ConfigConstants.VAL_DEF_ALLOWED_DELAYED_MSG_COUNT);
            Preconditions.checkArgument(
                    (this.linkMaxAllowedDelayedMsgCount >= ConfigConstants.VAL_MIN_ALLOWED_DELAYED_MSG_COUNT),
                    ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT + " must be >= "
                            + ConfigConstants.VAL_MIN_ALLOWED_DELAYED_MSG_COUNT);
            this.sessionWarnDelayedMsgCount = context.getLong(
                    ConfigConstants.SESSION_WARN_DELAYED_MSG_COUNT,
                    ConfigConstants.VAL_DEF_SESSION_WARN_DELAYED_MSG_COUNT);
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
            this.requestTimeoutMs = context.getInteger(ConfigConstants.CLIENT_REQUEST_TIMEOUT_MS,
                    ConfigConstants.VAL_DEF_REQUEST_TIMEOUT_MS);
            Preconditions.checkArgument((this.requestTimeoutMs >= ConfigConstants.VAL_MIN_REQUEST_TIMEOUT_MS),
                    ConfigConstants.CLIENT_REQUEST_TIMEOUT_MS + " must be >= "
                            + ConfigConstants.VAL_MIN_REQUEST_TIMEOUT_MS);
        } else {
            // initial controller configure
            this.controllerConfigFile = context.getString(CONTROLLER_CONFIG_FILE);
            initControlerAgentConfig();
        }
    }

    @Override
    public void start() {
        logger.info("{} sink is starting...", this.cachedSinkName);
        if (getChannel() == null) {
            logger.error("{}'s channel is null", this.cachedSinkName);
        }
        cachedMsgChannel = getChannel();
        // init monitor logic
        if (enableFileMetric) {
            this.monitorStats = new MonitorStats(this.cachedSinkName + "_stats",
                    CommonConfigHolder.getInstance().getFileMetricEventOutName()
                            + AttrConstants.SEP_HASHTAG + this.cachedSinkName,
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorStats.start();
        }
        if (this.sendRemote) {
            // initial message duplicate cache
            this.msgIdCache = new MsgIdCache(enableDeDupCheck,
                    visitConcurLevel, initCacheCapacity, expiredDurSec);
            // message dispatch queue
            this.dispatchQueue = new BufferQueue<>(maxInflightBufferSIzeInKB);
            if (enableFileMetric) {
                this.detailIndex = new MonitorIndex(this.cachedSinkName + "_detail_index",
                        CommonConfigHolder.getInstance().getFileMetricSinkOutName(),
                        CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                        CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
                this.detailIndex.start();
                this.sumIndex = new MonitorSumIndex(this.cachedSinkName + "_sum_index",
                        CommonConfigHolder.getInstance().getFileMetricSinkOutName(),
                        CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                        CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
                this.sumIndex.start();
            }
            // initial tube client
            try {
                TubeClientConfig conf = initTubeConfig();
                this.sessionFactory = new TubeMultiSessionFactory(conf);
            } catch (Throwable e) {
                stop();
                logger.error("{} create session factory failure, please re-check. ex2 {}",
                        this.cachedSinkName, e.getMessage());
                throw new FlumeException("create session factory failure, "
                        + "maybe tube master set error/shutdown in progress, please re-check");
            }
            this.reloadMetaConfig();
            // start message process logic
            for (int i = 0; i < sinkThreadPool.length; i++) {
                sinkThreadPool[i] = new Thread(new SinkTask(), cachedSinkName + "_sender-" + i);
                sinkThreadPool[i].start();
            }
        } else {
            try {
                createConnection(true);
            } catch (Throwable e) {
                logger.error("Unable to create {} controller client", this.cachedSinkName, e);
                destroyConnection();
                stop();
            }
        }
        this.mqClusterStarted = true;
        super.start();
        logger.info("{} sink is started", this.cachedSinkName);
    }

    /**
     * process event from channel
     *
     * @return  Status
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        if (isShutdown) {
            return Status.BACKOFF;
        }
        // wait mq cluster started
        while (!mqClusterStarted) {
            try {
                Thread.sleep(MQ_CLUSTER_STATUS_WAIT_DUR_MS);
            } catch (InterruptedException e1) {
                return Status.BACKOFF;
            } catch (Throwable e2) {
                //
            }
        }
        Transaction tx = cachedMsgChannel.getTransaction();
        tx.begin();
        try {
            Event event = cachedMsgChannel.take();
            // no data
            if (event == null) {
                tx.commit();
                return Status.BACKOFF;
            }
            // post event to dispatch queue
            if (event instanceof SimpleEvent
                    || event instanceof SinkRspEvent) {
                // memory event
                fileMetricIncSumStats(StatConstants.SINK_INDEX_EVENT_TAKE_SUCCESS);
            } else {
                // file event
                fileMetricIncSumStats(StatConstants.SINK_INDEX_FILE_TAKE_SUCCESS);
            }
            if (this.sendRemote) {
                acquireAndOfferDispatchedRecord(new EventProfile(event, true));
            } else {
                sendIndexToController(new EventProfile(event, true));
            }
            tx.commit();
            return Status.READY;
        } catch (Throwable t) {
            fileMetricIncSumStats(StatConstants.SINK_INDEX_EVENT_TAKE_FAILURE);
            if (logSendMsgCounter.shouldPrint()) {
                logger.warn("{} process event failed!", this.cachedSinkName, t);
            }
            try {
                tx.rollback();
            } catch (Throwable e) {
                if (logSendMsgCounter.shouldPrint()) {
                    logger.warn("{} channel take transaction rollback exception", this.cachedSinkName, e);
                }
            }
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public void stop() {
        logger.info("{} sink is stopping...", this.cachedSinkName);
        // stop fetch events
        this.isShutdown = true;
        if (sendRemote) {
            // wait event dispatch
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
            // stop sink worker thread pool
            if (sinkThreadPool != null) {
                for (Thread thread : sinkThreadPool) {
                    if (thread != null) {
                        thread.interrupt();
                    }
                }
                sinkThreadPool = null;
            }
            // close message duplicate cache
            msgIdCache.clearMsgIdCache();
            // close session factory
            if (sessionFactory != null) {
                try {
                    sessionFactory.shutdown();
                } catch (Throwable e) {
                    logger.error("{} destroy session factory error: {}", cachedSinkName, e.getMessage());
                }
                sessionFactory = null;
            }
        } else {
            // process sub-class logic
            destroyConnection();
        }
        // stop file statistic index
        if (enableFileMetric) {
            if (monitorStats != null) {
                monitorStats.stop();
            }
            if (sendRemote) {
                this.detailIndex.stop();
                this.sumIndex.stop();
            }
        }
        super.stop();
        logger.info("{} sink is stopped", this.cachedSinkName);
    }

    private void sendIndexToController(EventProfile profile) throws Exception {
        // check task whether invalid
        if (profile.isInValidTask()) {
            fileMetricIncWithDetailStats(
                    StatConstants.EVENT_SINK_CHANNEL_INVALID_DROPPED, profile.getGroupId());
            profile.clear();
            return;
        }
        // parse fields in headers
        profile.parseFields();
        if (!profile.isValidIndexMsg()) {
            fileMetricIncSumStats(StatConstants.SINK_INDEX_ILLEGAL_DROPPED);
            profile.clear();
            return;
        }
        // process message
        if (profile.isStatusIndex()) {
            try {
                int sentCnt = 0;
                FileStatus fs = FileStatusUtil.toFileStatus(profile.getEventBody());
                String fsStr = fs.toString();
                do {
                    if (this.isShutdown) {
                        throw new Exception("Sink node is shut down!");
                    }
                    if (!verifyConnection()) {
                        sleepSomeTime(maxSendFailureWaitDurMs);
                        continue;
                    }
                    sentCnt++;
                    try {
                        controllerAgent.reportFileStatus(fs);
                        fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_SEND_SUCCESS);
                        profile.clear();
                        break;
                    } catch (Throwable ex) {
                        // just retry
                        if (maxMsgRetries > 0 && sentCnt > maxMsgRetries) {
                            logger.warn(
                                    "ControllerSink filestatus: '{}' is skipped because of exceeded maxRetryCnt {}, ex {}",
                                    fsStr, maxMsgRetries, ex);
                            fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_OVERMAX_DROOPED);
                            return;
                        }
                        if (logAgentCounter.shouldPrint()) {
                            logger.warn("{} send message to Controller failure", cachedSinkName, ex);
                        }
                        // sleep some time
                        sleepSomeTime(maxSendFailureWaitDurMs);
                    }
                } while (maxMsgRetries < 0 || sentCnt < maxMsgRetries);
            } catch (Throwable ex) {
                destroyConnection();
                fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_SEND_EXCEPTION);
                String s = (profile.getEventBody() != null ? new String(profile.getEventBody()) : "");
                logger.warn("{} send status event failure, header is {}, body is {}",
                        cachedSinkName, profile.getProperties(), s, ex);
            }
        } else {
            // agent-measure
            AgentMeasureLogger.logMeasureInfo(profile.getEventBody());
            fileMetricIncSumStats(StatConstants.SINK_MEASURE_INDEX_OUTPUT_SUCCESS);
            profile.clear();
        }
    }

    private class SinkTask implements Runnable {

        @Override
        public void run() {
            logger.info("{} start send message logic: {}",
                    cachedSinkName, Thread.currentThread().getName());
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
                    if (logSendMsgCounter.shouldPrint()) {
                        logger.warn("{} send message logic {} throw exception: ",
                                cachedSinkName, Thread.currentThread().getName(), e1);
                    }
                    // sleep some time
                    sleepSomeTime(maxSendFailureWaitDurMs);
                }
            }
            logger.info("{} exits send message logic: {}",
                    cachedSinkName, Thread.currentThread().getName());
        }

        private void sendMessage(EventProfile profile) {
            // check task whether invalid
            if (profile.isInValidTask()) {
                releaseAcquiredSizePermit(profile.getMsgSize());
                fileMetricIncWithDetailStats(
                        StatConstants.EVENT_SINK_CHANNEL_INVALID_DROPPED, profile.getGroupId());
                profile.clear();
                return;
            }
            // parse fields in headers
            profile.parseFields();
            if (!profile.isValidIndexMsg()) {
                releaseAcquiredSizePermit(profile.getMsgSize());
                fileMetricIncSumStats(StatConstants.SINK_INDEX_ILLEGAL_DROPPED);
                profile.clear();
                return;
            }
            // check duplicate
            if (msgIdCache.cacheIfAbsent(profile.getMsgSeqId())) {
                releaseAcquiredSizePermit(profile.getMsgSize());
                fileMetricIncSumStats(StatConstants.SINK_INDEX_DUPLICATE_DROOPED);
                if (logDupMsgPrinter.shouldPrint()) {
                    logger.info("{} package {} existed,just discard.",
                            cachedSinkName, profile.getMsgSeqId());
                }
                profile.clear();
                return;
            }
            // build message
            long sendTime;
            Message message;
            try {
                if (profile.isBringEvent()) {
                    String attr = "bid=" + profile.getGroupId()
                            + "&tid=" + profile.getStreamId()
                            + "&dt=" + System.currentTimeMillis()
                            + "&NodeIP=" + profile.getClientIp();
                    InLongMsg inLongMsg = InLongMsg.newInLongMsg(false);
                    inLongMsg.addMsg(attr, profile.getEventBody());
                    // build message
                    message = new Message(agentIndexTopic, inLongMsg.buildArray());
                    message.putSystemHeader((profile.isStatusIndex() ? agentStatusTid : agentMeasureTid),
                            DateTimeUtils.ms2yyyyMMddHHmm(profile.getPkgTime()));
                    message.setAttrKeyVal(AttributeConstants.RCV_TIME,
                            profile.getProperties().get(AttributeConstants.RCV_TIME));
                    message.setAttrKeyVal(ConfigConstants.MSG_ENCODE_VER, msgEncodeTypeId);
                    message.setAttrKeyVal(EventConstants.HEADER_KEY_VERSION, msgEncodeTypeId);
                    message.setAttrKeyVal(ConfigConstants.REMOTE_IP_KEY, profile.getClientIp());
                    message.setAttrKeyVal(ConfigConstants.DATAPROXY_IP_KEY, profile.getDataProxyIp());
                    sendTime = System.currentTimeMillis();
                    message.setAttrKeyVal(ConfigConstants.MSG_SEND_TIME, String.valueOf(sendTime));
                    EventProfile newProfile = new EventProfile(profile, message);
                    // send message
                    messageProducer.sendMessage(message,
                            new MyCallback(newProfile, sendTime, agentIndexTopic));
                    profile.clear();
                } else {
                    // send message
                    sendTime = profile.updateSendTime();
                    message = profile.getMessage();
                    messageProducer.sendMessage(message,
                            new MyCallback(profile, sendTime, agentIndexTopic));
                }
            } catch (Throwable ex) {
                fileMetricIncWithDetailStats((profile.isStatusIndex()
                        ? StatConstants.SINK_STATUS_INDEX_SEND_EXCEPTION
                        : StatConstants.SINK_MEASURE_INDEX_SEND_EXCEPTION), agentIndexTopic);
                processSendFail(profile, DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
                if (logSendMsgCounter.shouldPrint()) {
                    logger.error("{} send remote message to {} throw exception!",
                            cachedSinkName, agentIndexTopic, ex);
                }
            }
        }
    }

    private class MyCallback implements MessageSentCallback {

        private final EventProfile profile;
        private final long sendTime;
        private final String topic;

        public MyCallback(EventProfile profile, long sendTime, String topic) {
            this.profile = profile;
            this.sendTime = sendTime;
            this.topic = topic;
        }

        @Override
        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                fileMetricAddSuccStats(profile, topic, result.getPartition().getHost());
                releaseAcquiredSizePermit(profile.getMsgSize());
                addSendResultMetric(profile);
                profile.ack();
            } else {
                fileMetricAddFailStats(profile, topic,
                        result.getPartition().getHost(), topic + "." + result.getErrCode());
                processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, result.getErrMsg());
                if (logSendMsgCounter.shouldPrint()) {
                    logger.warn("{} remote message send to {} failed: {}",
                            cachedSinkName, topic, result.getErrMsg());
                }
            }
        }

        @Override
        public void onException(Throwable ex) {
            fileMetricAddExceptStats(profile, topic, "", topic);
            processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
            if (logSendMsgCounter.shouldPrint()) {
                logger.warn("{} remote message send to {} tube exception",
                        cachedSinkName, topic, ex);
            }
        }
    }

    private void sleepSomeTime(long someTime) {
        // sleep some time
        try {
            Thread.sleep(someTime);
        } catch (Throwable e2) {
            //
        }
    }

    /**
     * processSendFail
     */
    public void processSendFail(EventProfile profile, DataProxyErrCode errCode, String errMsg) {
        msgIdCache.invalidCache(profile.getMsgSeqId());
        if (profile.isResend(enableRetryAfterFailure, maxMsgRetries)) {
            offerDispatchRecord(profile);
            fileMetricIncSumStats(profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_FAILRETRY
                    : StatConstants.SINK_MEASURE_INDEX_REMOTE_FAILRETRY);
        } else {
            releaseAcquiredSizePermit(profile.getMsgSize());
            fileMetricIncSumStats(profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_FAILDROPPED
                    : StatConstants.SINK_MEASURE_INDEX_REMOTE_FAILDROPPED);
            profile.fail(errCode, errMsg);
            profile.clear();
        }
    }

    private void fileMetricIncSumStats(String eventKey) {
        if (enableFileMetric) {
            monitorStats.incSumStats(eventKey);
        }
    }

    private void fileMetricIncWithDetailStats(String eventKey, String detailInfoKey) {
        if (enableFileMetric) {
            monitorStats.incSumStats(eventKey);
            monitorStats.incDetailStats(eventKey + "#" + detailInfoKey);
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

    private void reloadMetaConfig() {
        if (!sendRemote || sessionFactory == null || messageProducer != null) {
            return;
        }
        try {
            messageProducer = sessionFactory.createProducer();
        } catch (Throwable e1) {
            logger.warn("{} create producer failure", cachedSinkName, e1);
        }
        Set<String> subSet = new HashSet<>();
        subSet.add(agentIndexTopic);
        try {
            messageProducer.publish(subSet);
        } catch (Throwable e) {
            if (logPubTopicPrinter.shouldPrint()) {
                logger.warn("{} publish topics failure, topics = {}", cachedSinkName, subSet, e);
            }
        }
        logger.info("{} publishTopics {}", cachedSinkName, subSet);
    }

    private void initControlerAgentConfig() {
        FileReader fr = null;
        try {
            if (controllerProp != null) {
                controllerProp.clear();
                controllerProp = null;
            }
            controllerProp = new Properties();
            fr = new FileReader(controllerConfigFile);
            controllerProp.load(fr);
        } catch (FileNotFoundException e) {
            logger.error("ControllerSink can not find conf file {}", controllerConfigFile);
        } catch (IOException e) {
            logger.error("ControllerSink can not read conf file {}", controllerConfigFile);
        } finally {
            try {
                if (fr != null) {
                    fr.close();
                }
            } catch (Exception e) {
                logger.error("ControllerSink retAgent file {} close error, ex {}",
                        controllerConfigFile, e);
            }
        }
    }

    /**
     * Ensure the connection exists and is active. If the connection is not
     * active, destroy it and recreate it.
     *
     * @throws FlumeException If there are errors closing or opening the RPC connection.
     */
    private boolean verifyConnection() throws FlumeException {
        if (controllerAgent != null && controllerAgent.isActive()) {
            return true;
        } else {
            return createConnection(false);
        }
    }

    /**
     * If this function is called successively without calling {@see
     * #destroyConnection()}, only the first call has any effect.
     *
     * @throws ControllerException if an RPC client connection could not be opened
     */
    private boolean createConnection(boolean needthrow) throws ControllerException {
        boolean isReCreate = false;
        if (controllerAgent != null) {
            isReCreate = true;
            if (controllerAgent.isActive()) {
                return true;
            }
            destroyConnection();
        }
        try {
            controllerAgent = ControllerRpcClientFactory.getInstance(controllerProp);
            fileMetricIncSumStats(StatConstants.SINK_INDEX_AGENT_CREATE_SUCCESS);
            if (isReCreate) {
                logger.info("{} created controller connnection {}", cachedSinkName, controllerProp);
            } else {
                logger.info("{} re-created controller connnection {}", cachedSinkName, controllerProp);
            }
            return true;
        } catch (Throwable ex) {
            controllerAgent = null;
            fileMetricIncSumStats(StatConstants.SINK_INDEX_AGENT_CREATE_EXCEPTION);
            if (logAgentCounter.shouldPrint()) {
                logger.error("Attempt to create controller failed.", ex);
            }
            if (needthrow) {
                if (ex instanceof ControllerException) {
                    throw (ControllerException) ex;
                } else {
                    throw new ControllerException(ex);
                }
            }
            return false;
        }
    }

    private void destroyConnection() {
        try {
            if (controllerAgent != null) {
                controllerAgent.close();
                controllerAgent = null;
                fileMetricIncSumStats(StatConstants.SINK_INDEX_AGENT_DESTROY_SUCCESS);
            }
        } catch (Throwable e) {
            controllerAgent = null;
            fileMetricIncSumStats(StatConstants.SINK_INDEX_AGENT_DESTROY_EXCEPTION);
            if (logAgentCounter.shouldPrint()) {
                logger.error("{} attempt to close controller agent failed.", cachedSinkName, e);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("{} closed controller connection", cachedSinkName);
        }
    }

    public void acquireAndOfferDispatchedRecord(EventProfile record) {
        this.dispatchQueue.acquire(record.getMsgSize());
        this.dispatchQueue.offer(record);
    }

    public void offerDispatchRecord(EventProfile record) {
        this.dispatchQueue.offer(record);
    }

    public EventProfile pollDispatchedRecord() {
        return this.dispatchQueue.pollRecord();
    }

    public EventProfile takeDispatchedRecord() {
        return this.dispatchQueue.takeRecord();
    }

    public void releaseAcquiredSizePermit(long msgSize) {
        this.dispatchQueue.release(msgSize);
    }

    public int getDispatchQueueSize() {
        return this.dispatchQueue.size();
    }

    public int getDispatchAvailablePermits() {
        return this.dispatchQueue.availablePermits();
    }

    public void fileMetricAddSuccStats(EventProfile profile, String topic, String brokerIP) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, true, topic, brokerIP,
                (profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_SUCCESS
                        : StatConstants.SINK_MEASURE_INDEX_REMOTE_SUCCESS),
                "");
    }

    public void fileMetricAddFailStats(EventProfile profile, String topic, String brokerIP, String detailKey) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, false, topic, brokerIP,
                (profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_FAILURE
                        : StatConstants.SINK_MEASURE_INDEX_REMOTE_SUCCESS),
                detailKey);
    }

    public void fileMetricAddExceptStats(EventProfile profile, String topic, String brokerIP, String detailKey) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, false, topic, brokerIP,
                (profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_EXCEPTION
                        : StatConstants.SINK_MEASURE_INDEX_REMOTE_EXCEPTION),
                detailKey);
    }

    private void fileMetricIncStats(EventProfile profile, boolean isSucc,
            String topic, String brokerIP, String eventKey, String detailInfoKey) {
        String tenMinsDt = DateTimeUtils.ms2yyyyMMddHHmmTenMins(profile.getDt());
        String tenMinsPkgTime = DateTimeUtils.ms2yyyyMMddHHmmTenMins(profile.getPkgTime());
        StringBuilder statsKey = new StringBuilder(512)
                .append(cachedSinkName)
                .append(AttrConstants.SEP_HASHTAG).append(profile.getGroupId())
                .append(AttrConstants.SEP_HASHTAG).append(profile.getStreamId())
                .append(AttrConstants.SEP_HASHTAG).append(topic)
                .append(AttrConstants.SEP_HASHTAG).append(AttrConstants.SEP_HASHTAG)
                .append(profile.getDataProxyIp());
        String sumKey = statsKey.toString()
                + AttrConstants.SEP_HASHTAG + tenMinsDt
                + AttrConstants.SEP_HASHTAG + tenMinsPkgTime;
        statsKey.append(AttrConstants.SEP_HASHTAG).append(brokerIP)
                .append(AttrConstants.SEP_HASHTAG).append(tenMinsDt)
                .append(AttrConstants.SEP_HASHTAG).append(
                        DateTimeUtils.ms2yyyyMMddHHmm(profile.getPkgTime()));
        if (isSucc) {
            detailIndex.addSuccStats(statsKey.toString(), profile.getMsgCnt(), 1, profile.getMsgSize());
            sumIndex.addSuccStats(sumKey, profile.getMsgCnt(), 1, profile.getMsgSize());
            monitorStats.incSumStats(eventKey);
        } else {
            detailIndex.addFailStats(statsKey.toString(), 1);
            sumIndex.addFailStats(sumKey, 1);
            monitorStats.incSumStats(eventKey);
            monitorStats.incDetailStats(eventKey + "#" + detailInfoKey);
        }
    }
    /**
     * addSendResultMetric
     */
    public void addSendResultMetric(EventProfile profile) {
        AuditUtils.addTDBus(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, profile);
    }
}
