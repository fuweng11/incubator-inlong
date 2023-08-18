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
import org.apache.inlong.dataproxy.base.SinkRspEvent;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.dataproxy.metrics.stats.MonitorIndex;
import org.apache.inlong.dataproxy.metrics.stats.MonitorStats;
import org.apache.inlong.dataproxy.metrics.stats.MonitorSumIndex;
import org.apache.inlong.dataproxy.utils.BufferQueue;
import org.apache.inlong.dataproxy.utils.DateTimeUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseSink extends AbstractSink implements Configurable, ConfigUpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(BaseSink.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);
    // cached sink's name
    protected String cachedSinkName;
    // cached sink's channel
    protected Channel cachedMsgChannel;
    // whether the sink has closed
    protected volatile boolean isShutdown = false;
    // whether mq cluster connected
    protected volatile boolean mqClusterStarted = false;
    // mq cluster status wait duration
    private final long MQ_CLUSTER_STATUS_WAIT_DUR_MS = 2000L;
    // message send thread count
    protected int maxThreads;
    // message send thread pool
    protected Thread[] sinkThreadPool;
    // max inflight buffer size in KB
    private int maxInflightBufferSIzeInKB;
    // event dispatch queue
    protected BufferQueue<EventProfile> dispatchQueue;
    // file metric statistic
    protected boolean enableFileMetric;
    protected MonitorIndex detailIndex = null;
    protected MonitorSumIndex sumIndex = null;
    protected MonitorStats monitorStats = null;
    private int maxMonitorCnt;
    // whether to resend the message after sending failure
    protected boolean enableRetryAfterFailure;
    // Maximum number of retries to send
    protected int maxRetries;
    // Sink sending status statistics
    private long statIntvlMillS = 60000L;
    // meta configure change lister thread
    private Thread configListener;
    // configure change notify
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final AtomicLong lastNotifyTime = new AtomicLong(0);
    // message duplicate cache
    private boolean enableDeDupCheck = true;
    private int visitConcurLevel = 32;
    private int initCacheCapacity = 5000000;
    private long expiredDurSec = 30;
    protected MsgIdCache msgIdCache;

    public BaseSink() {

    }

    @Override
    public void configure(Context context) {
        logger.info("{} start to configure, context:{}.", getName(), context.toString());
        this.cachedSinkName = getName();
        this.enableFileMetric = CommonConfigHolder.getInstance().isEnableFileMetric();
        this.enableRetryAfterFailure = CommonConfigHolder.getInstance().isEnableSendRetryAfterFailure();
        this.maxRetries = CommonConfigHolder.getInstance().getMaxRetriesAfterFailure();
        this.maxMonitorCnt = context.getInteger(
                ConfigConstants.MAX_MONITOR_CNT, ConfigConstants.DEF_MONITOR_STAT_CNT);
        this.maxThreads = context.getInteger(ConfigConstants.MAX_THREADS, 10);
        // sink worker thread pool
        this.sinkThreadPool = new Thread[maxThreads];
        this.statIntvlMillS = context.getLong(ConfigConstants.STAT_INTERVAL_SEC, 60000L);
        Preconditions.checkArgument(statIntvlMillS >= 0, "statIntvlMillS must be >= 0");
        this.enableDeDupCheck = context.getBoolean(ConfigConstants.ENABLE_MSG_CACHE_DEDUP,
                ConfigConstants.DEF_ENABLE_MSG_CACHE_DEDUP);
        this.visitConcurLevel = context.getInteger(ConfigConstants.MAX_CACHE_CONCURRENT_ACCESS,
                ConfigConstants.DEF_MAX_CACHE_CONCURRENT_ACCESS);
        this.expiredDurSec = context.getLong(ConfigConstants.MAX_CACHE_SURVIVED_TIME_SEC,
                ConfigConstants.DEF_MAX_CACHE_SURVIVED_TIME_SEC);
        this.initCacheCapacity = context.getInteger(ConfigConstants.MAX_CACHE_SURVIVED_SIZE,
                ConfigConstants.DEF_MAX_CACHE_SURVIVED_SIZE);
        this.initCacheCapacity = context.getInteger(ConfigConstants.MAX_CACHE_SURVIVED_SIZE,
                ConfigConstants.DEF_MAX_CACHE_SURVIVED_SIZE);
        this.maxInflightBufferSIzeInKB = context.getInteger(ConfigConstants.MAX_INFLIGHT_BUFFER_QUEUE_SIZE_KB,
                CommonConfigHolder.getInstance().getMaxBufferQueueSizeKb());
        // linkMaxAllowedDelayedMsgCount = context.getLong(ConfigConstants.LINK_MAX_ALLOWED_DELAYED_MSG_COUNT,80000L);
        // //单连接最大
        // sessionWarnDelayedMsgCount = context.getLong(ConfigConstants.SESSION_WARN_DELAYED_MSG_COUNT,2000000L);
        // //原先为100万，当超过这个值的时候启动负载均衡，即排序，选top20%最高的broker不发送
        // sessionMaxAllowedDelayedMsgCount =
        // context.getLong(ConfigConstants.SESSION_MAX_ALLOWED_DELAYED_MSG_COUNT,4000000L);//总会话最大消息，超过这个值会被forbidden
        // nettyWriteBufferHighWaterMark =
        // context.getLong(ConfigConstants.NETTY_WRITE_BUFFER_HIGH_WATER_MARK,15*1024*1024L);//原先为10m，tube netty缓冲区最大值
        // recoverthreadcount
        // =context.getInteger(ConfigConstants.RECOVER_THREAD_COUNT,Runtime.getRuntime().availableProcessors()+1);
    }

    @Override
    public void start() {
        logger.info("{} sink is starting...", this.cachedSinkName);
        if (getChannel() == null) {
            logger.error("{}'s channel is null", this.cachedSinkName);
        }
        cachedMsgChannel = getChannel();
        // register sink to config-manager
        ConfigManager.getInstance().regMetaConfigChgCallback(this);
        // initial message duplicate cache
        this.msgIdCache = new MsgIdCache(enableDeDupCheck,
                visitConcurLevel, initCacheCapacity, expiredDurSec);
        // message dispatch queue
        this.dispatchQueue = new BufferQueue<>(maxInflightBufferSIzeInKB);
        // init monitor logic
        if (enableFileMetric) {
            this.detailIndex = new MonitorIndex(CommonConfigHolder.getInstance().getFileMetricSourceOutName(),
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.detailIndex.start();
            this.sumIndex = new MonitorSumIndex(CommonConfigHolder.getInstance().getFileMetricSourceOutName(),
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.sumIndex.start();
            this.monitorStats = new MonitorStats(
                    CommonConfigHolder.getInstance().getFileMetricEventOutName()
                            + AttrConstants.SEP_HASHTAG + this.cachedSinkName,
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorStats.start();
        }
        // start sub-class sink process
        startSinkProcess();
        // start configure change listener thread
        this.configListener = new Thread(new ConfigChangeProcessor());
        this.configListener.setName(this.cachedSinkName + "-configure-listener");
        this.configListener.start();
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
                fileMetricIncSumStats(StatConstants.EVENT_SINK_EVENT_TAKE_SUCCESS);
            } else {
                // file event
                fileMetricIncSumStats(StatConstants.EVENT_SINK_EVENT_V0_FILE);
            }
            acquireAndOfferDispatchedRecord(new EventProfile(event));
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
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public void stop() {
        logger.info("{} sink is stopping...", this.cachedSinkName);
        this.isShutdown = true;
        // stop configure listener thread
        if (this.configListener != null) {
            try {
                this.configListener.interrupt();
                configListener.join();
                this.configListener = null;
            } catch (Throwable ee) {
                //
            }
        }
        // process sub-class logic
        stopSinkProcess();
        // stop sink worker thread pool
        if (sinkThreadPool != null) {
            for (Thread thread : sinkThreadPool) {
                if (thread != null) {
                    thread.interrupt();
                }
            }
            sinkThreadPool = null;
        }
        // stop file statistic index
        if (enableFileMetric) {
            if (detailIndex != null) {
                detailIndex.stop();
            }
            if (sumIndex != null) {
                sumIndex.stop();
            }
            if (monitorStats != null) {
                monitorStats.stop();
            }
        }
        super.stop();
        logger.info("{} sink is stopped", this.cachedSinkName);
    }

    @Override
    public void update() {
        reentrantLock.lock();
        try {
            lastNotifyTime.set(System.currentTimeMillis());
            condition.signal();
        } finally {
            reentrantLock.unlock();
        }
    }

    public abstract void startSinkProcess();

    public abstract void stopSinkProcess();

    public abstract void reloadMetaConfig();

    public boolean isMqClusterStarted() {
        return mqClusterStarted;
    }

    public void setMQClusterStarted() {
        this.mqClusterStarted = true;
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

    public void fileMetricIncSumStats(String eventKey) {
        if (enableFileMetric) {
            monitorStats.incSumStats(eventKey);
        }
    }

    public void fileMetricIncWithDetailStats(String eventKey, String detailInfoKey) {
        if (enableFileMetric) {
            monitorStats.incSumStats(eventKey);
            monitorStats.incDetailStats(eventKey + "#" + detailInfoKey);
        }
    }

    public void fileMetricAddSuccStats(EventProfile profile, String topic, String brokerIP) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, true, topic, brokerIP, StatConstants.EVENT_SINK_SUCCESS, "");
    }

    public void fileMetricAddFailStats(EventProfile profile, String topic, String brokerIP, String detailKey) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, false, topic, brokerIP, StatConstants.EVENT_SINK_FAILURE, detailKey);
    }

    public void fileMetricAddExceptStats(EventProfile profile, String topic, String brokerIP, String detailKey) {
        if (!enableFileMetric) {
            return;
        }
        fileMetricIncStats(profile, false, topic, brokerIP, StatConstants.EVENT_SINK_RECEIVEEXCEPT, detailKey);
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
                .append(AttrConstants.SEP_HASHTAG)
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
     * ConfigChangeProcessor
     *
     * Metadata configuration change listener class, when the metadata change notification
     * arrives, check and change the mapping relationship between the mq cluster information
     * and the configured inlongid to Topic,
     */
    private class ConfigChangeProcessor implements Runnable {

        @Override
        public void run() {
            long lastCheckTime;
            logger.info("{} config-change processor start!", cachedSinkName);
            while (!isShutdown) {
                reentrantLock.lock();
                try {
                    condition.await();
                } catch (InterruptedException e1) {
                    logger.info("{} config-change processor meet interrupt, break!", cachedSinkName);
                    break;
                } finally {
                    reentrantLock.unlock();
                }
                do {
                    lastCheckTime = lastNotifyTime.get();
                    reloadMetaConfig();
                } while (lastCheckTime != lastNotifyTime.get());
            }
            logger.info("{} config-change processor exit!", cachedSinkName);
        }
    }

    /**
     * addSendResultMetric
     */
    public void addSendResultMetric(EventProfile profile) {
        AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, profile.getEvent());
    }
}
