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

package org.apache.inlong.dataproxy.sink.mq;

import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.dataproxy.sink.common.SinkContext;
import org.apache.inlong.dataproxy.utils.BufferQueue;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.INLONG_COMPRESSED_TYPE;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * MessageQueueZoneSinkContext
 */
public class MessageQueueZoneSinkContext extends SinkContext {

    public static final String KEY_NODE_ID = "nodeId";
    public static final String PREFIX_PRODUCER = "producer.";
    public static final String KEY_COMPRESS_TYPE = "compressType";

    private final BufferQueue<BatchPackProfile> dispatchQueue;

    private final String proxyClusterId;
    private final String nodeId;
    private final Context producerContext;
    //
    private final INLONG_COMPRESSED_TYPE compressType;

    /**
     * Constructor
     */
    public MessageQueueZoneSinkContext(String sinkName, Context context, Channel channel,
            BufferQueue<BatchPackProfile> dispatchQueue) {
        super(sinkName, context, channel);
        this.dispatchQueue = dispatchQueue;
        // proxyClusterId
        this.proxyClusterId = CommonConfigHolder.getInstance().getClusterName();
        // nodeId
        this.nodeId = CommonConfigHolder.getInstance().getProxyNodeId();
        // compressionType
        String strCompressionType = CommonConfigHolder.getInstance().getMsgCompressType();
        this.compressType = INLONG_COMPRESSED_TYPE.valueOf(strCompressionType);
        // producerContext
        Map<String, String> producerParams = context.getSubProperties(PREFIX_PRODUCER);
        this.producerContext = new Context(producerParams);
    }

    /**
     * start
     */
    public void start() {
        super.start();
    }

    /**
     * close
     */
    public void close() {
        super.close();
    }

    /**
     * get proxyClusterId
     * 
     * @return the proxyClusterId
     */
    public String getProxyClusterId() {
        return proxyClusterId;
    }

    /**
     * get dispatchQueue
     * 
     * @return the dispatchQueue
     */
    public BufferQueue<BatchPackProfile> getDispatchQueue() {
        return dispatchQueue;
    }

    /**
     * get producerContext
     * 
     * @return the producerContext
     */
    public Context getProducerContext() {
        return producerContext;
    }

    /**
     * get compressType
     * 
     * @return the compressType
     */
    public INLONG_COMPRESSED_TYPE getCompressType() {
        return compressType;
    }

    /**
     * get nodeId
     * 
     * @return the nodeId
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * addSendResultMetric
     */
    public void addSendResultMetric(BatchPackProfile currentRecord, String mqName, String topic, boolean result,
            long sendTime) {
        if (currentRecord instanceof SimpleBatchPackProfileV0) {
            AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS,
                    ((SimpleBatchPackProfileV0) currentRecord).getSimpleProfile());
            return;
        }

        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_DATA_ID, "-");
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, mqName);
        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, topic);
        final long currentTime = System.currentTimeMillis();
        currentRecord.getEvents().forEach(event -> {
            long msgTime = event.getMsgTime();
            long auditFormatTime =
                    msgTime - msgTime % CommonConfigHolder.getInstance().getAuditFormatInvlMs();
            dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
            DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
            if (result) {
                metricItem.sendSuccessCount.addAndGet(1);
                metricItem.sendSuccessSize.addAndGet(event.getBody().length);
                if (sendTime > 0) {
                    long sinkDuration = currentTime - sendTime;
                    long nodeDuration = currentTime - event.getSourceTime();
                    long wholeDuration = currentTime - msgTime;
                    metricItem.sinkDuration.addAndGet(sinkDuration);
                    metricItem.nodeDuration.addAndGet(nodeDuration);
                    metricItem.wholeDuration.addAndGet(wholeDuration);
                }
                AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, event);
            } else {
                metricItem.sendFailCount.addAndGet(1);
                metricItem.sendFailSize.addAndGet(event.getBody().length);
            }
        });
    }

    /**
     * addSendMetric
     */
    public void addSendMetric(BatchPackProfile currentRecord, String mqName, String topic, int sendPackSize) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_DATA_ID, "-");
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, mqName);
        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime = currentRecord.getDispatchTime();
        long auditFormatTime =
                msgTime - msgTime % CommonConfigHolder.getInstance().getAuditFormatInvlMs();
        dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = currentRecord.getCount();
        long size = currentRecord.getSize();
        metricItem.sendCount.addAndGet(count);
        metricItem.sendSize.addAndGet(size);
        metricItem.sendPackCount.incrementAndGet();
        metricItem.sendPackSize.addAndGet(sendPackSize);
    }

    /**
     * addReadFailMetric
     */
    public void addSendFailMetric() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_DATA_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_INLONG_GROUP_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_INLONG_STREAM_ID, "-");
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, "-");
        long msgTime = System.currentTimeMillis();
        long auditFormatTime =
                msgTime - msgTime % CommonConfigHolder.getInstance().getAuditFormatInvlMs();
        dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.sendFailCount.incrementAndGet();
        metricItem.sendFailSize.incrementAndGet();
    }

    /**
     * fillInlongId
     */
    public static void fillInlongId(BatchPackProfile currentRecord, Map<String, String> dimensions) {
        String inlongGroupId = currentRecord.getInlongGroupId();
        inlongGroupId = (StringUtils.isBlank(inlongGroupId)) ? "-" : inlongGroupId;
        String inlongStreamId = currentRecord.getInlongStreamId();
        inlongStreamId = (StringUtils.isBlank(inlongStreamId)) ? "-" : inlongStreamId;
        dimensions.put(DataProxyMetricItem.KEY_INLONG_GROUP_ID, inlongGroupId);
        dimensions.put(DataProxyMetricItem.KEY_INLONG_STREAM_ID, inlongStreamId);
    }

    /**
     * processSendFail
     */
    public void processSendFail(BatchPackProfile currentRecord, String mqName, String topic, long sendTime) {
        if (currentRecord.isResend()) {
            dispatchQueue.offer(currentRecord);
            fileMetricEventInc(StatConstants.EVENT_SINK_FAILRETRY);
            this.addSendResultMetric(currentRecord, mqName, topic, false, sendTime);
        } else {
            currentRecord.fail();
            fileMetricEventInc(StatConstants.EVENT_SINK_FAILDROPPED);
        }
    }

    /**
     * createCacheClusterSelector
     */
    public CacheClusterSelector createCacheClusterSelector() {
        String strSelectorClass = CommonConfigHolder.getInstance().getCacheClusterSelector();
        try {
            Class<?> selectorClass = ClassUtils.getClass(strSelectorClass);
            Object selectorObject = selectorClass.getDeclaredConstructor().newInstance();
            if (selectorObject instanceof Configurable) {
                Configurable configurable = (Configurable) selectorObject;
                configurable.configure(new Context(CommonConfigHolder.getInstance().getProperties()));
            }
            if (selectorObject instanceof CacheClusterSelector) {
                CacheClusterSelector selector = (CacheClusterSelector) selectorObject;
                return selector;
            }
        } catch (Throwable t) {
            LOG.error("Fail to init CacheClusterSelector,selectorClass:{},error:{}",
                    strSelectorClass, t.getMessage(), t);
        }
        return null;
    }
}
