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
import org.apache.inlong.common.enums.DataProxyMsgEncType;
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
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.factory.TubeMultiSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentCallback;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;

import com.google.common.base.Preconditions;
import com.tencent.tdbank.ac.controller.ControllerException;
import com.tencent.tdbank.ac.controller.avro.FileStatus;
import com.tencent.tdbank.ac.controller.avro.FileStatusUtil;
import com.tencent.tdbank.ac.controller.client.ControllerRpcClient;
import com.tencent.tdbank.ac.controller.client.ControllerRpcClientFactory;
import org.apache.commons.lang3.math.NumberUtils;
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
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);
    private static final LogCounter logDupMsgPrinter = new LogCounter(10, 100000, 30 * 1000);

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
        if (!this.sendRemote) {
            this.maxThreads = 1;
        }
        // initial sink worker thread pool
        this.sinkThreadPool = new Thread[this.maxThreads];
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
        // initial message duplicate cache
        this.msgIdCache = new MsgIdCache(enableDeDupCheck,
                visitConcurLevel, initCacheCapacity, expiredDurSec);
        // message dispatch queue
        this.dispatchQueue = new BufferQueue<>(maxInflightBufferSIzeInKB);
        // init monitor logic
        if (enableFileMetric) {
            this.monitorStats = new MonitorStats(this.cachedSinkName + "_stats",
                    CommonConfigHolder.getInstance().getFileMetricEventOutName()
                            + AttrConstants.SEP_HASHTAG + this.cachedSinkName,
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorStats.start();
            if (this.sendRemote) {
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
        }
        if (this.sendRemote) {
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
        } else {
            try {
                createConnection();
            } catch (Throwable e) {
                logger.error("Unable to create {} controller client", this.cachedSinkName, e);
                destroyConnection();
                stop();
            }
        }
        // start message process logic
        for (int i = 0; i < sinkThreadPool.length; i++) {
            sinkThreadPool[i] = new Thread(new SinkTask(), cachedSinkName + "_sender-" + i);
            sinkThreadPool[i].start();
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
            acquireAndOfferDispatchedRecord(new EventProfile(event, true));
            tx.commit();
            return Status.READY;
        } catch (Throwable t) {
            fileMetricIncSumStats(StatConstants.SINK_INDEX_EVENT_TAKE_FAILURE);
            if (logCounter.shouldPrint()) {
                logger.error("{} process event failed!", this.cachedSinkName, t);
            }
            try {
                tx.rollback();
            } catch (Throwable e) {
                if (logCounter.shouldPrint()) {
                    logger.error("{} channel take transaction rollback exception", this.cachedSinkName, e);
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
        if (sendRemote) {
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
        super.stop();
        logger.info("{} sink is stopped", this.cachedSinkName);
    }

    private class SinkTask implements Runnable {

        @Override
        public void run() {
            logger.info("{} task {} start send message logic.",
                    cachedSinkName, Thread.currentThread().getName());
            EventProfile profile = null;
            while (canSend) {
                try {
                    if (!sendRemote) {
                        verifyConnection();
                    }
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
                    if (!sendRemote) {
                        destroyConnection();
                    }
                    if (logCounter.shouldPrint()) {
                        logger.error("{} - {} send message failure", cachedSinkName,
                                Thread.currentThread().getName(), e1);
                    }
                    // sleep some time
                    try {
                        Thread.sleep(maxSendFailureWaitDurMs);
                    } catch (Throwable e2) {
                        //
                    }
                }
            }
            logger.info("{} task {} exits send message logic.",
                    cachedSinkName, Thread.currentThread().getName());
        }

        private void sendMessage(EventProfile profile) {
            // check duplicate
            String msgType = profile.getProperties().get(ConfigConstants.INDEX_MSG_TYPE);
            if (msgType == null || !(msgType.equals(ConfigConstants.INDEX_TYPE_FILE_STATUS)
                    || msgType.equals(ConfigConstants.INDEX_TYPE_MEASURE))) {
                fileMetricIncSumStats(StatConstants.SINK_INDEX_ILLEGAL_DROPPED);
                return;
            }
            String msgSeqId = profile.getProperties().get(ConfigConstants.SEQUENCE_ID);
            if (msgIdCache.cacheIfAbsent(msgSeqId)) {
                fileMetricIncSumStats(StatConstants.SINK_INDEX_DUPLICATE_DROOPED);
                if (logDupMsgPrinter.shouldPrint()) {
                    logger.info("{} package {} existed,just discard.", cachedSinkName, msgSeqId);
                }
                return;
            }
            // check event type
            if (profile.getIsStatusIndex() == null) {
                profile.setStatusIndex(msgType.equals(ConfigConstants.INDEX_TYPE_FILE_STATUS));
            }
            // build message
            if (sendRemote) {
                String attr = "bid=" + profile.getGroupId()
                        + "&tid=" + profile.getStreamId()
                        + "&dt=" + System.currentTimeMillis()
                        + "&NodeIP=" + profile.getProperties().get(ConfigConstants.REMOTE_IP_KEY);
                InLongMsg inLongMsg = InLongMsg.newInLongMsg(false);
                inLongMsg.addMsg(attr, profile.getEventBody());
                // build message
                long dataTimeL = Long.parseLong(profile.getProperties().get(ConfigConstants.PKG_TIME_KEY));
                Message message = new Message(agentIndexTopic, inLongMsg.buildArray());
                message.putSystemHeader((profile.isStatusIndex() ? agentStatusTid : agentMeasureTid),
                        DateTimeUtils.ms2yyyyMMddHHmm(dataTimeL));
                message.setAttrKeyVal(AttributeConstants.RCV_TIME,
                        profile.getProperties().get(AttributeConstants.RCV_TIME));
                message.setAttrKeyVal(ConfigConstants.MSG_ENCODE_VER,
                        DataProxyMsgEncType.MSG_ENCODE_TYPE_INLONGMSG.getStrId());
                message.setAttrKeyVal(EventConstants.HEADER_KEY_VERSION,
                        DataProxyMsgEncType.MSG_ENCODE_TYPE_INLONGMSG.getStrId());
                message.setAttrKeyVal(ConfigConstants.REMOTE_IP_KEY,
                        profile.getProperties().get(ConfigConstants.REMOTE_IP_KEY));
                message.setAttrKeyVal(ConfigConstants.DATAPROXY_IP_KEY,
                        profile.getProperties().get(ConfigConstants.DATAPROXY_IP_KEY));
                long sendTime = System.currentTimeMillis();
                message.setAttrKeyVal(ConfigConstants.MSG_SEND_TIME, String.valueOf(sendTime));
                try {
                    messageProducer.sendMessage(message, new MyCallback(profile, sendTime, agentIndexTopic));
                } catch (Throwable ex) {
                    fileMetricIncWithDetailStats((profile.isStatusIndex()
                            ? StatConstants.SINK_STATUS_INDEX_SEND_EXCEPTION
                            : StatConstants.SINK_MEASURE_INDEX_SEND_EXCEPTION), agentIndexTopic);
                    processSendFail(profile, DataProxyErrCode.SEND_REQUEST_TO_MQ_FAILURE, ex.getMessage());
                    if (logCounter.shouldPrint()) {
                        logger.error("{} send remote message throw exception!", cachedSinkName, ex);
                    }
                }
            } else {
                if (profile.isStatusIndex()) {
                    try {
                        int sentCnt = 0;
                        FileStatus fs = FileStatusUtil.toFileStatus(profile.getEventBody());
                        String fsStr = fs.toString();
                        do {
                            sentCnt++;
                            try {
                                controllerAgent.reportFileStatus(fs);
                                fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_SEND_SUCCESS);
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
                                fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_SEND_FAILRETRY);
                            }
                        } while (maxMsgRetries < 0 || sentCnt < maxMsgRetries);
                    } catch (Throwable ex) {
                        fileMetricIncSumStats(StatConstants.SINK_STATUS_INDEX_SEND_EXCEPTION);
                        String s = (profile.getEventBody() != null ? new String(profile.getEventBody()) : "");
                        logger.error("{} send status event failure, header is {}, body is {}",
                                cachedSinkName, profile.getProperties(), s, ex);
                    }
                } else {
                    // agent-measure
                    AgentMeasureLogger.logMeasureInfo(profile.getEventBody());
                    fileMetricIncSumStats(StatConstants.SINK_MEASURE_INDEX_OUTPUT_SUCCESS);
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
                releaseAcquiredSizePermit(profile);
                addSendResultMetric(profile);
                profile.ack();
            } else {
                fileMetricAddFailStats(profile, topic,
                        result.getPartition().getHost(), topic + "." + result.getErrCode());
                processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, result.getErrMsg());
                if (logCounter.shouldPrint()) {
                    logger.warn("{} remote message send failed: {}", cachedSinkName, result.getErrMsg());
                }
            }
        }

        @Override
        public void onException(Throwable ex) {
            fileMetricAddExceptStats(profile, topic, "", topic);
            processSendFail(profile, DataProxyErrCode.MQ_RETURN_ERROR, ex.getMessage());
            if (logCounter.shouldPrint()) {
                logger.error("{} remote message send to {} tube exception", cachedSinkName, topic, ex);
            }
        }
    }

    /**
     * processSendFail
     */
    public void processSendFail(EventProfile profile, DataProxyErrCode errCode, String errMsg) {
        msgIdCache.invalidCache(profile.getProperties().get(ConfigConstants.SEQUENCE_ID));
        if (profile.isResend(enableRetryAfterFailure, maxMsgRetries)) {
            offerDispatchRecord(profile);
            fileMetricIncSumStats(profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_FAILRETRY
                    : StatConstants.SINK_MEASURE_INDEX_REMOTE_FAILRETRY);
        } else {
            releaseAcquiredSizePermit(profile);
            fileMetricIncSumStats(profile.isStatusIndex() ? StatConstants.SINK_STATUS_INDEX_REMOTE_FAILDROPPED
                    : StatConstants.SINK_MEASURE_INDEX_REMOTE_FAILDROPPED);
            profile.fail(errCode, errMsg);
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
            if (logCounter.shouldPrint()) {
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
    private void verifyConnection() throws FlumeException {
        if (controllerAgent == null || !controllerAgent.isActive()) {
            createConnection();
        }
    }

    /**
     * If this function is called successively without calling {@see
     * #destroyConnection()}, only the first call has any effect.
     *
     * @throws ControllerException if an RPC client connection could not be opened
     */
    private void createConnection() throws ControllerException {
        boolean isReCreate = false;
        if (controllerAgent != null) {
            isReCreate = true;
            if (controllerAgent.isActive()) {
                return;
            } else {
                try {
                    controllerAgent.close();
                    controllerAgent = null;
                } catch (Exception ex) {
                    // ignore
                }
            }
        }

        try {
            controllerAgent = ControllerRpcClientFactory.getInstance(controllerProp);
        } catch (Throwable ex) {
            if (ex instanceof ControllerException) {
                throw (ControllerException) ex;
            } else {
                throw new ControllerException(ex);
            }
        }
        if (isReCreate) {
            logger.info("{} created controller connnection {}", cachedSinkName, controllerProp);
        } else {
            logger.info("{} re-created controller connnection {}", cachedSinkName, controllerProp);
        }
    }

    private void destroyConnection() {
        try {
            if (controllerAgent != null) {
                controllerAgent.close();
                controllerAgent = null;
            }
        } catch (Throwable e) {
            logger.error("Attempt to close {} client failed.", cachedSinkName, e);
        }
        logger.debug("{} closed controller connection", cachedSinkName);
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

    public void releaseAcquiredSizePermit(EventProfile record) {
        this.dispatchQueue.release(record.getMsgSize());
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
        long pkgTimeL = Long.parseLong(profile.getProperties().get(ConfigConstants.PKG_TIME_KEY));
        String tenMinsDt = DateTimeUtils.ms2yyyyMMddHHmmTenMins(profile.getDt());
        String tenMinsPkgTime = DateTimeUtils.ms2yyyyMMddHHmmTenMins(pkgTimeL);
        StringBuilder statsKey = new StringBuilder(512)
                .append(cachedSinkName)
                .append(AttrConstants.SEP_HASHTAG).append(profile.getGroupId())
                .append(AttrConstants.SEP_HASHTAG).append(profile.getStreamId())
                .append(AttrConstants.SEP_HASHTAG).append(topic)
                .append(AttrConstants.SEP_HASHTAG).append(AttrConstants.SEP_HASHTAG)
                .append(profile.getProperties().get(ConfigConstants.DATAPROXY_IP_KEY));
        String sumKey = statsKey.toString()
                + AttrConstants.SEP_HASHTAG + tenMinsDt
                + AttrConstants.SEP_HASHTAG + tenMinsPkgTime;
        statsKey.append(AttrConstants.SEP_HASHTAG).append(brokerIP)
                .append(AttrConstants.SEP_HASHTAG).append(tenMinsDt)
                .append(AttrConstants.SEP_HASHTAG).append(DateTimeUtils.ms2yyyyMMddHHmm(pkgTimeL));
        if (isSucc) {
            int msgCnt = NumberUtils.toInt(
                    profile.getProperties().get(ConfigConstants.MSG_COUNTER_KEY), 1);
            detailIndex.addSuccStats(statsKey.toString(), msgCnt, 1, profile.getMsgSize());
            sumIndex.addSuccStats(sumKey, msgCnt, 1, profile.getMsgSize());
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
        AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, profile.getEvent());
    }
}
