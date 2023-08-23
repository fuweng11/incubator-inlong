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

import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.dataproxy.base.SinkRspEvent;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.metrics.stats.MonitorStats;
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

public abstract class ControllerSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ControllerSink.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);
    private static final LogCounter logDupMsgPrinter = new LogCounter(10, 100000, 30 * 1000);

    private static String CONTROLLER_CONFIG_FILE = "controller-config-file";
    private static final String MAX_RETRY_CNT_STR = "max-retry-cnt";
    private static final int VAL_DEF_RETRY_CNT_STR = 3;

    private static final String SEND_REMOTE = "send-remote";
    private static final boolean VAL_DEF_SEND_REMOTE = false;

    private static final String LOG_TOPIC = "agent-log-topic";
    private static final String VAL_DEF_LOG_TOPIC = "teg_tdbank";

    private static final String LOG_TID = "agent-log-tid";
    private static final String VAL_DEF_LOG_TID = "agent_measure_log";

    private static final String LOG_BID = "agent-log-bid";
    private static final String VAL_DEF_LOG_BID = "b_teg_tdbank";

    // cached sink's name
    protected String cachedSinkName;
    // cached sink's channel
    protected Channel cachedMsgChannel;
    // whether the sink has closed
    protected volatile boolean isShutdown = false;
    // whether mq cluster connected
    protected volatile boolean mqClusterStarted = false;
    // message duplicate cache
    private boolean enableDeDupCheck = true;
    private int visitConcurLevel = 32;
    private int initCacheCapacity = 5000000;
    private long expiredDurSec = 30;
    protected MsgIdCache msgIdCache;

    // clusterMasterAddress
    protected String clusterAddrList;
    // controller agent
    private ControllerRpcClient controllerAgent;
    private String controllerConfigFile;
    private Properties controllerProp;
    private boolean sendRemote = false;
    private TubeMultiSessionFactory sessionFactory = null;
    private MessageProducer messageProducer = null;
    private String agentLogTopic;
    private String agentLogBid;
    private String agentLogTid;
    private long linkMaxAllowedDelayedMsgCount;
    private long sessionWarnDelayedMsgCount;
    private long sessionMaxAllowedDelayedMsgCount;
    private long nettyWriteBufferHighWaterMark;
    // rpc request timeout ms
    protected int requestTimeoutMs;
    // max retry times if send failure
    protected int maxMsgRetries;
    // mq cluster status wait duration
    private final long MQ_CLUSTER_STATUS_WAIT_DUR_MS = 2000L;
    // file metric statistic
    protected boolean enableFileMetric;
    protected MonitorStats monitorStats = null;

    public ControllerSink() {

    }

    @Override
    public void configure(Context context) {
        this.cachedSinkName = getName();
        logger.info("{} start to configure, context:{}.", this.cachedSinkName, context.toString());
        this.enableFileMetric = CommonConfigHolder.getInstance().isEnableFileMetric();
        // initial controller configure
        this.controllerConfigFile = context.getString(CONTROLLER_CONFIG_FILE);
        initControlerAgentConfig();
        // initial send retry count
        Integer myRetryCnt = context.getInteger(MAX_RETRY_CNT_STR);
        if (myRetryCnt == null) {
            this.maxMsgRetries = VAL_DEF_RETRY_CNT_STR;
        } else {
            if (myRetryCnt < 0) {
                this.maxMsgRetries = Integer.MAX_VALUE;
            } else if (myRetryCnt == 0) {
                this.maxMsgRetries = 1;
            } else {
                this.maxMsgRetries = myRetryCnt;
            }
        }

        this.maxMsgRetries = context.getInteger(MAX_RETRY_CNT_STR,
                CommonConfigHolder.getInstance().getMaxRetriesAfterFailure());
        // initial send mode
        this.sendRemote = context.getBoolean(SEND_REMOTE, VAL_DEF_SEND_REMOTE);
        if (sendRemote) {
            this.clusterAddrList = context.getString(ConfigConstants.MASTER_SERVER_URL_LIST);
            Preconditions.checkState(clusterAddrList != null,
                    ConfigConstants.MASTER_SERVER_URL_LIST + " parameter not specified");
            agentLogTopic = context.getString(LOG_TOPIC, VAL_DEF_LOG_TOPIC);
            agentLogBid = context.getString(LOG_BID, VAL_DEF_LOG_BID);
            agentLogTid = context.getString(LOG_TID, VAL_DEF_LOG_TID);
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

        // init monitor logic
        if (enableFileMetric) {
            this.monitorStats = new MonitorStats(this.cachedSinkName + "_stats",
                    CommonConfigHolder.getInstance().getFileMetricEventOutName()
                            + AttrConstants.SEP_HASHTAG + this.cachedSinkName,
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorStats.start();
        }
        try {
            createConnection();
        } catch (Throwable e) {
            logger.error("Unable to create {} controller client", cachedSinkName, e);
            destroyConnection();
            stop();
        }
        // initial tube client
        if (sendRemote) {
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
            this.reloadMetaConfig();
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
        try {
            verifyConnection();
            tx.begin();
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
                fileMetricIncSumStats(StatConstants.EVENT_SINK_EVENT_TAKE_SUCCESS);
            } else {
                // file event
                fileMetricIncSumStats(StatConstants.EVENT_SINK_EVENT_V0_FILE);
            }
            processEvent(event);
            tx.commit();
            return Status.READY;
        } catch (Throwable t) {
            fileMetricIncSumStats(StatConstants.EVENT_SINK_EVENT_TAKE_FAILURE);
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
            destroyConnection();
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public void stop() {
        logger.info("{} sink is stopping...", this.cachedSinkName);
        this.isShutdown = true;
        // process sub-class logic
        destroyConnection();
        // shutdown session factory
        if (sendRemote) {
            if (sessionFactory != null) {
                try {
                    sessionFactory.shutdown();
                } catch (Throwable e) {
                    logger.error("{} destroy session factory error: {}", cachedSinkName, e.getMessage());
                }
                sessionFactory = null;
            }
        }
        // stop file statistic index
        if (enableFileMetric) {
            if (monitorStats != null) {
                monitorStats.stop();
            }
        }
        // close message duplicate cache
        msgIdCache.clearMsgIdCache();
        super.stop();
        logger.info("{} sink is stopped", this.cachedSinkName);
    }

    private void processEvent(Event event) {
        // check duplicate
        String msgType = event.getHeaders().get(ConfigConstants.INDEX_MSG_TYPE);
        if (msgType == null || !(msgType.equals(ConfigConstants.INDEX_TYPE_FILE_STATUS)
                || msgType.equals(ConfigConstants.INDEX_TYPE_MEASURE))) {
            fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_NON_INDEX);
            return;
        }
        String msgSeqId = event.getHeaders().get(ConfigConstants.SEQUENCE_ID);
        if (msgIdCache.cacheIfAbsent(msgSeqId)) {
            fileMetricIncSumStats(StatConstants.EVENT_SINK_MESSAGE_DUPLICATE);
            if (logDupMsgPrinter.shouldPrint()) {
                logger.info("{} package {} existed,just discard.", cachedSinkName, msgSeqId);
            }
            return;
        }
        // build message
        if (msgType.equals(ConfigConstants.INDEX_TYPE_FILE_STATUS)) {
            try {
                FileStatus fs = FileStatusUtil.toFileStatus(event.getBody());
                String fsStr = fs.toString();
                for (int i = 0; i < maxMsgRetries; ++i) {
                    try {
                        controllerAgent.reportFileStatus(fs);
                        break;
                    } catch (Throwable ex) {
                        // just retry
                        if (i == maxMsgRetries) {
                            logger.warn(
                                    "ControllerSink filestatus: '{}' is skipped because of exceeded maxRetryCnt {}, ex {}",
                                    new Object[]{fsStr, maxMsgRetries, ex});
                            fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_RETRY_OVER);
                            return;
                        }
                    }
                }
                fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_SEND_SUCCESS);
            } catch (Throwable e) {
                fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_SEND_EXCEPTION);
                String s = (event.getBody() != null ? new String(event.getBody()) : "");
                logger.error("bad event header is " + event.getHeaders() + ", body is " + s);
            }
        } else {
            // agent-measure
            AgentMeasureLogger.logMeasureInfo(event.getBody());
            if (sendRemote) {
                String attr = "bid=" + agentLogBid
                        + "&tid=" + agentLogTid
                        + "&dt=" + System.currentTimeMillis()
                        + "&m=9&NodeIP=" + event.getHeaders().get(ConfigConstants.REMOTE_IP_KEY);
                InLongMsg inLongMsg = InLongMsg.newInLongMsg(false);
                inLongMsg.addMsg(attr, event.getBody());
                Message message = new Message(agentLogTopic, inLongMsg.buildArray());
                message.setAttrKeyVal("tdbusip", event.getHeaders().get(ConfigConstants.DATAPROXY_IP_KEY));
                try {
                    messageProducer.sendMessage(message, new MessageCallback());
                } catch (Throwable e) {
                    fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_REMOTE_EXCEPTION);
                    if (logCounter.shouldPrint()) {
                        logger.error("{} send remote message throw exception!", cachedSinkName, e);
                    }
                }
            }
        }
    }

    private class MessageCallback implements MessageSentCallback {

        MessageCallback() {
        }

        @Override
        public void onMessageSent(MessageSentResult result) {
            if (result.isSuccess()) {
                fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_REMOTE_SUCCESS);

            } else {
                logger.warn("{} remote message send failed: {}", cachedSinkName, result.getErrMsg());
                fileMetricIncWithDetailStats(StatConstants.EVENT_SINK_INDEX_REMOTE_FAILURE,
                        agentLogTopic + "." + result.getErrCode());
            }
        }

        @Override
        public void onException(Throwable e) {
            fileMetricIncSumStats(StatConstants.EVENT_SINK_INDEX_REMOTE_EXCEPTION);
            logger.error("{} remote message send failed!", cachedSinkName, e);
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
        subSet.add(agentLogTopic);
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
}
