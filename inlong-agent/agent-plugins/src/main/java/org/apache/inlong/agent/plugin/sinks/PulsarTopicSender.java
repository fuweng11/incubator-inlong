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

package org.apache.inlong.agent.plugin.sinks;

import org.apache.inlong.agent.conf.AgentConfiguration;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_BLOCK_IF_QUEUE_FULL;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_COMPRESSION_TYPE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_BATCH_BYTES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_BATCH_INTERVAL_MILLIS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_BATCH_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_PENDING_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITION;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PRODUCER_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_CLIENT_ENABLE_BATCH;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_CLIENT_TIMEOUT_SECOND;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_BLOCK_IF_QUEUE_FULL;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_COMPRESSION_TYPE;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_ENABLE_BATCH;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_MAX_BATCH_BYTES;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_MAX_BATCH_INTERVAL_MILLIS;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_MAX_BATCH_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_MAX_PENDING_MESSAGES;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_MAX_PENDING_MESSAGES_ACROSS_PARTITION;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_PRODUCER_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_TIMEOUT_SECOND;

public class PulsarTopicSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTopicSender.class);
    private final AtomicInteger producerIndex = new AtomicInteger(0);
    private final PulsarClient pulsarClient;
    private List<Producer> producers;
    private boolean enableBatch;
    private boolean blockIfQueueFull;
    private int maxPendingMessages;
    private int maxPendingMessagesAcrossPartitions;
    private CompressionType compressionType;
    private int maxBatchingBytes;
    private int maxBatchingMessages;
    private long maxBatchingPublishDelayMillis;
    private int sendTimeoutSecond;
    private int producerNum;

    private String topic;
    private String dbJobId;

    public PulsarTopicSender(PulsarClient client, String topic, String dbJobId) {
        AgentConfiguration agentConf = AgentConfiguration.getAgentConf();
        this.sendTimeoutSecond = agentConf.getInt(PULSAR_CLIENT_TIMEOUT_SECOND, DEFAULT_PULSAR_CLIENT_TIMEOUT_SECOND);
        this.enableBatch = agentConf.getBoolean(PULSAR_CLIENT_ENABLE_BATCH, DEFAULT_PULSAR_CLIENT_ENABLE_BATCH);
        this.blockIfQueueFull = agentConf.getBoolean(PULSAR_CLIENT_BLOCK_IF_QUEUE_FULL, DEFAULT_BLOCK_IF_QUEUE_FULL);
        this.maxPendingMessages = agentConf.getInt(PULSAR_CLIENT_MAX_PENDING_MESSAGES, DEFAULT_MAX_PENDING_MESSAGES);
        this.maxBatchingBytes = agentConf.getInt(PULSAR_CLIENT_MAX_BATCH_BYTES, DEFAULT_MAX_BATCH_BYTES);
        this.maxBatchingMessages = agentConf.getInt(PULSAR_CLIENT_MAX_BATCH_MESSAGES, DEFAULT_MAX_BATCH_MESSAGES);
        this.maxBatchingPublishDelayMillis = agentConf.getInt(PULSAR_CLIENT_MAX_BATCH_INTERVAL_MILLIS,
                DEFAULT_MAX_BATCH_INTERVAL_MILLIS);
        this.maxPendingMessagesAcrossPartitions = agentConf.getInt(PULSAR_CLIENT_MAX_PENDING_MESSAGES_ACROSS_PARTITION,
                DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITION);
        this.producerNum = agentConf.getInt(PULSAR_CLIENT_PRODUCER_NUM, DEFAULT_PRODUCER_NUM);
        String compression = agentConf.get(PULSAR_CLIENT_COMPRESSION_TYPE, DEFAULT_COMPRESSION_TYPE);
        if (StringUtils.isNotEmpty(compression)) {
            this.compressionType = CompressionType.valueOf(compression);
        } else {
            this.compressionType = CompressionType.NONE;
        }
        this.pulsarClient = client;
        this.topic = topic;
        this.dbJobId = dbJobId;
        initProducer(producerNum);
    }

    public Producer getProducer() {
        if (CollectionUtils.isEmpty(producers)) {
            LOGGER.error("job[{}] empty producers", dbJobId);
            return null;
        }
        if (producerIndex.getAndIncrement() == Integer.MAX_VALUE) {
            producerIndex.set(0);
        }
        int index = (producerIndex.getAndIncrement() & Integer.MAX_VALUE) % producers.size();
        return producers.get(index);
    }

    /**
     * close all pulsar producer and shutdown client
     */
    public void close() {
        if (CollectionUtils.isEmpty(producers)) {
            return;
        }
        for (Producer producer : producers) {
            try {
                producer.close();
            } catch (Throwable e) {
                LOGGER.error("job[{}] close pulsar producer error", dbJobId, e);
            }
        }
    }

    private void initProducer(int producerNum) {
        producers = new ArrayList<>(producerNum);
        for (int i = 0; i < producerNum; i++) {
            Producer<byte[]> producer = createProducer();
            if (producer != null) {
                producers.add(producer);
            }
        }
    }

    private Producer<byte[]> createProducer() {
        try {
            return pulsarClient.newProducer().topic(topic)
                    .sendTimeout(sendTimeoutSecond, TimeUnit.SECONDS)
                    .topic(topic)
                    .enableBatching(enableBatch)
                    .blockIfQueueFull(blockIfQueueFull)
                    .maxPendingMessages(maxPendingMessages)
                    .maxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions)
                    .compressionType(compressionType)
                    .batchingMaxMessages(maxBatchingMessages)
                    .batchingMaxBytes(maxBatchingBytes)
                    .batchingMaxPublishDelay(maxBatchingPublishDelayMillis, TimeUnit.MILLISECONDS)
                    .create();
        } catch (Throwable e) {
            LOGGER.error("Create producer[topic:{}] error", topic, e);
            return null;
        }
    }

}
