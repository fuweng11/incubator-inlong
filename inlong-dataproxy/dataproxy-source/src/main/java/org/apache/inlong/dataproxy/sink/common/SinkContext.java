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

package org.apache.inlong.dataproxy.sink.common;

import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItemSet;
import org.apache.inlong.dataproxy.metrics.stats.MonitorIndex;
import org.apache.inlong.dataproxy.metrics.stats.MonitorStats;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.pulsar.PulsarHandler;

import org.apache.commons.lang.ClassUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SinkContext
 */
public class SinkContext {

    public static final String KEY_MAX_THREADS = "maxThreads";
    public static final String KEY_PROCESSINTERVAL = "processInterval";
    public static final String KEY_RELOADINTERVAL = "reloadInterval";
    public static final String KEY_MESSAGE_QUEUE_HANDLER = "messageQueueHandler";

    protected static final Logger logger = LoggerFactory.getLogger(SinkContext.class);

    protected final String clusterId;
    protected final String sinkName;
    protected final Context sinkContext;

    protected final Channel channel;
    //
    protected final int maxThreads;
    protected final long processInterval;
    protected final long reloadInterval;
    //
    protected final DataProxyMetricItemSet metricItemSet;
    // file metric statistic
    protected MonitorIndex monitorIndex = null;
    private MonitorStats monitorStats = null;

    /**
     * Constructor
     */
    public SinkContext(String sinkName, Context context, Channel channel) {
        this.sinkName = sinkName;
        this.sinkContext = context;
        this.channel = channel;
        this.clusterId = CommonConfigHolder.getInstance().getClusterName();
        this.maxThreads = sinkContext.getInteger(KEY_MAX_THREADS, 10);
        this.processInterval = sinkContext.getInteger(KEY_PROCESSINTERVAL, 100);
        this.reloadInterval = sinkContext.getLong(KEY_RELOADINTERVAL, 60000L);
        //
        this.metricItemSet = new DataProxyMetricItemSet(sinkName);
        MetricRegister.register(this.metricItemSet);
    }

    /**
     * start
     */
    public void start() {
        // init monitor logic
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            this.monitorIndex = new MonitorIndex(CommonConfigHolder.getInstance().getFileMetricSinkOutName(),
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorStats = new MonitorStats(
                    CommonConfigHolder.getInstance().getFileMetricEventOutName()
                            + AttrConstants.SEP_HASHTAG + this.getSinkName(),
                    CommonConfigHolder.getInstance().getFileMetricStatInvlSec() * 1000L,
                    CommonConfigHolder.getInstance().getFileMetricStatCacheCnt());
            this.monitorIndex.start();
            this.monitorStats.start();
        }
    }

    /**
     * close
     */
    public void close() {
        // stop file statistic index
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            if (monitorIndex != null) {
                monitorIndex.stop();
            }
            if (monitorStats != null) {
                monitorStats.stop();
            }
        }
    }

    public void fileMetricIncSumStats(String eventKey) {
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            monitorStats.incSumStats(eventKey);
        }
    }

    public void fileMetricIncWithDetailStats(String eventKey, String detailInfoKey) {
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            monitorStats.incSumStats(eventKey);
            monitorStats.incDetailStats(eventKey + "#" + detailInfoKey);
        }
    }

    public void fileMetricAddSuccCnt(String key, int cnt, int packCnt, long packSize) {
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            monitorIndex.addSuccStats(key, cnt, packCnt, packSize);
        }
    }

    public void fileMetricAddFailCnt(String key, int failCnt) {
        if (CommonConfigHolder.getInstance().isEnableFileMetric()) {
            monitorIndex.addFailStats(key, failCnt);
        }
    }

    /**
     * get clusterId
     * 
     * @return the clusterId
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * get sinkName
     * 
     * @return the sinkName
     */
    public String getSinkName() {
        return sinkName;
    }

    /**
     * get sinkContext
     * 
     * @return the sinkContext
     */
    public Context getSinkContext() {
        return sinkContext;
    }

    /**
     * get channel
     *
     * @return the channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * get maxThreads
     * 
     * @return the maxThreads
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * get processInterval
     * 
     * @return the processInterval
     */
    public long getProcessInterval() {
        return processInterval;
    }

    /**
     * get reloadInterval
     * 
     * @return the reloadInterval
     */
    public long getReloadInterval() {
        return reloadInterval;
    }

    /**
     * get metricItemSet
     * 
     * @return the metricItemSet
     */
    public DataProxyMetricItemSet getMetricItemSet() {
        return metricItemSet;
    }

    /**
     * createEventHandler
     */
    public EventHandler createEventHandler() {
        // IEventHandler
        String eventHandlerClass = CommonConfigHolder.getInstance().getEventHandler();
        try {
            Class<?> handlerClass = ClassUtils.getClass(eventHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof EventHandler) {
                return (EventHandler) handlerObject;
            }
        } catch (Throwable t) {
            logger.error("{} fail to init EventHandler,handlerClass:{},error:{}",
                    this.sinkName, eventHandlerClass, t.getMessage(), t);
        }
        return null;
    }

    /**
     * createMessageQueueHandler
     */
    public MessageQueueHandler createMessageQueueHandler(CacheClusterConfig config) {
        String strHandlerClass = config.getParams().getOrDefault(KEY_MESSAGE_QUEUE_HANDLER,
                PulsarHandler.class.getName());
        logger.info("{}'s mq handler class = {}", this.sinkName, strHandlerClass);
        try {
            Class<?> handlerClass = ClassUtils.getClass(strHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof MessageQueueHandler) {
                return (MessageQueueHandler) handlerObject;
            }
        } catch (Throwable t) {
            logger.error("{} fail to init MessageQueueHandler,handlerClass:{},error:{}",
                    this.sinkName, strHandlerClass, t.getMessage(), t);
        }
        return null;
    }
}
