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

package org.apache.inlong.dataproxy.sink.mq.kafka;

import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.sink.common.EventHandler;
import org.apache.inlong.dataproxy.sink.mq.BatchPackProfile;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueHandler;
import org.apache.inlong.dataproxy.sink.mq.MessageQueueZoneSinkContext;
import org.apache.inlong.dataproxy.sink.mq.OrderBatchPackProfileV0;
import org.apache.inlong.dataproxy.sink.mq.SimpleBatchPackProfileV0;

import org.apache.commons.collections.MapUtils;
import org.apache.flume.Context;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * KafkaHandler
 * 
 */
public class KafkaHandler implements MessageQueueHandler {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaHandler.class);
    public static final String KEY_NAMESPACE = "namespace";

    private CacheClusterConfig config;
    private String clusterName;
    private MessageQueueZoneSinkContext sinkContext;

    // kafka producer
    private KafkaProducer<String, byte[]> producer;
    private ThreadLocal<EventHandler> handlerLocal = new ThreadLocal<>();

    /**
     * init
     * @param config
     * @param sinkContext
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
        // create kafka producer
        try {
            // prepare configuration
            Properties props = new Properties();
            Context context = this.sinkContext.getProducerContext();
            props.putAll(context.getParameters());
            props.putAll(config.getParams());
            LOG.info("try to create kafka client:{}", props);
            producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
            LOG.info("create new producer success:{}", producer);
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void publishTopic(Set<String> topicSet) {
        //
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        // kafka producer
        this.producer.close();
        LOG.info("kafka handler stopped");
    }

    /**
     * send
     * @param event
     * @return
     */
    @Override
    public boolean send(BatchPackProfile event) {
        try {
            // idConfig
            IdTopicConfig idConfig = ConfigManager.getInstance().getIdTopicConfig(
                    event.getInlongGroupId(), event.getInlongStreamId());
            if (idConfig == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOUID);
                sinkContext.addSendResultMetric(event, clusterName, event.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(event.getSize());
                event.fail();
                return false;
            }
            String topic = idConfig.getTopicName();
            if (topic == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOTOPIC);
                sinkContext.addSendResultMetric(event, clusterName, event.getUid(), false, 0);
                sinkContext.getDispatchQueue().release(event.getSize());
                event.fail();
                return false;
            }
            // create producer failed
            if (producer == null) {
                sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_NOPRODUCER);
                sinkContext.processSendFail(event, clusterName, topic, 0);
                return false;
            }
            // send
            if (event instanceof SimpleBatchPackProfileV0) {
                this.sendSimpleProfileV0((SimpleBatchPackProfileV0) event, idConfig, topic);
            } else if (event instanceof OrderBatchPackProfileV0) {
                this.sendOrderProfileV0((OrderBatchPackProfileV0) event, idConfig, topic);
            } else {
                this.sendProfileV1(event, idConfig, topic);
            }
            return true;
        } catch (Exception e) {
            sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SENDEXCEPT);
            sinkContext.processSendFail(event, clusterName, event.getUid(), 0);
            LOG.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * sendProfileV1
     */
    private void sendProfileV1(BatchPackProfile event, IdTopicConfig idConfig,
            String topic) throws Exception {
        EventHandler handler = handlerLocal.get();
        if (handler == null) {
            handler = this.sinkContext.createEventHandler();
            handlerLocal.set(handler);
        }
        // headers
        Map<String, String> headers = handler.parseHeader(idConfig, event, sinkContext.getNodeId(),
                sinkContext.getCompressType());
        // compress
        byte[] bodyBytes = handler.parseBody(idConfig, event, sinkContext.getCompressType());
        // metric
        sinkContext.addSendMetric(event, clusterName, topic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();

        // prepare ProducerRecord
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            producerRecord.headers().add(key, value.getBytes());
        });

        // callback
        Callback callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata arg0, Exception ex) {
                if (ex != null) {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                    sinkContext.processSendFail(event, clusterName, topic, sendTime);
                    LOG.error("Send ProfileV1 to Kafka failure", ex);
                } else {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(event, clusterName, topic, true, sendTime);
                    sinkContext.getDispatchQueue().release(event.getSize());
                    event.ack();
                }
            }
        };
        producer.send(producerRecord, callback);
    }

    /**
     * sendSimpleProfileV0
     */
    private void sendSimpleProfileV0(SimpleBatchPackProfileV0 event, IdTopicConfig idConfig,
            String topic) throws Exception {
        // headers
        Map<String, String> headers = event.getProperties();
        if (MapUtils.isEmpty(headers)) {
            headers = event.getSimpleProfile().getHeaders();
        }
        // body
        byte[] bodyBytes = event.getSimpleProfile().getBody();
        // metric
        sinkContext.addSendMetric(event, clusterName, topic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();

        // prepare ProducerRecord
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            producerRecord.headers().add(key, value.getBytes());
        });

        // callback
        Callback callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata arg0, Exception ex) {
                if (ex != null) {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                    sinkContext.processSendFail(event, clusterName, topic, sendTime);
                    LOG.error("Send SimpleProfileV0 to Kafka failure", ex);
                } else {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(event, clusterName, topic, true, sendTime);
                    sinkContext.getDispatchQueue().release(event.getSize());
                    event.ack();
                }
            }
        };
        producer.send(producerRecord, callback);
    }

    /**
     * sendOrderProfileV0
     */
    private void sendOrderProfileV0(OrderBatchPackProfileV0 event, IdTopicConfig idConfig,
            String topic) throws Exception {
        // headers
        Map<String, String> headers = event.getOrderProfile().getHeaders();
        // compress
        byte[] bodyBytes = event.getOrderProfile().getBody();
        // metric
        sinkContext.addSendMetric(event, clusterName, topic, bodyBytes.length);
        // sendAsync
        long sendTime = System.currentTimeMillis();

        // prepare ProducerRecord
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, bodyBytes);
        // add headers
        headers.forEach((key, value) -> {
            producerRecord.headers().add(key, value.getBytes());
        });

        // callback
        Callback callback = new Callback() {

            @Override
            public void onCompletion(RecordMetadata arg0, Exception ex) {
                if (ex != null) {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_RECEIVEEXCEPT);
                    sinkContext.processSendFail(event, clusterName, topic, sendTime);
                    LOG.error("Send OrderProfileV0 to Kafka failure", ex);
                } else {
                    sinkContext.fileMetricEventInc(StatConstants.EVENT_SINK_SUCCESS);
                    sinkContext.addSendResultMetric(event, clusterName, topic, true, sendTime);
                    sinkContext.getDispatchQueue().release(event.getSize());
                    event.ack();
                }
            }
        };
        producer.send(producerRecord, callback);
    }
}
