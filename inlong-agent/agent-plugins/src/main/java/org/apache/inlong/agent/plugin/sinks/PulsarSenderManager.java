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
import org.apache.inlong.agent.metrics.MetricReport;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_CLIENT_IO_TREHAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PULSAR_CONNECTION_PRE_BROKER;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CLIENT_IO_TREHAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.PULSAR_CONNECTION_PRE_BROKER;

public class PulsarSenderManager implements MetricReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSenderManager.class);
    private static volatile PulsarSenderManager pulsarSenderManager = null;
    private final AgentConfiguration agentConf = AgentConfiguration.getAgentConf();

    /*
     * pulsar client DBsync job 间隔离，同一个job 内的task 间共用，支持多集群 key : dbJobId + "_" + clusterUrl
     */
    private ConcurrentHashMap<String, PulsarClient> pulsarClientMap = new ConcurrentHashMap<>();

    /*
     * 每个topic 的producer ，在dbjob 内的task 间隔离，即每个task 有自己单独的producer ，复用agent 目前的read、channel、sink模式 key : dbJobId + "_" +
     * threadId + "_" + topic;
     */
    private ConcurrentHashMap<String, List<PulsarTopicSender>> pulsarTopicSenderMap = new ConcurrentHashMap<>();

    private int clientIoThreads;
    private int connectionsPreBroker;
    private String dbJobId;

    public PulsarSenderManager(String dbJobId) {
        // agentConf
        this.dbJobId = dbJobId;
        clientIoThreads = agentConf.getInt(PULSAR_CLIENT_IO_TREHAD_NUM, DEFAULT_PULSAR_CLIENT_IO_TREHAD_NUM);
        connectionsPreBroker = agentConf.getInt(PULSAR_CONNECTION_PRE_BROKER, DEFAULT_PULSAR_CONNECTION_PRE_BROKER);
    }

    public PulsarTopicSender getPulsarTopicSender(int index, String dbJobId, String threadId, String topic,
            List<PulsarClient> pulsarClients) {
        String key = getPulsarSenderMapKey(threadId, topic);
        List<PulsarTopicSender> pulsarTopicSenders = pulsarTopicSenderMap.compute(key, (k, v) -> {
            if ((v == null || v.size() == 0) && !CollectionUtils.isEmpty(pulsarClients)) {
                List<PulsarTopicSender> senders = new ArrayList<>();
                for (PulsarClient pulsarClient : pulsarClients) {
                    senders.add(new PulsarTopicSender(pulsarClient, topic, dbJobId));
                }
                v = senders;
            }
            return v;
        });
        return selectProducer(index, pulsarTopicSenders);
    }

    public void destoryPulsarTopicSender() {
        if (pulsarTopicSenderMap != null) {
            pulsarTopicSenderMap.forEach((k, v) -> {
                if (v != null) {
                    v.stream().forEach((vv) -> {
                        vv.close();
                    });
                }
            });
            pulsarTopicSenderMap.clear();
        }

    }

    private PulsarTopicSender selectProducer(int index, List<PulsarTopicSender> pulsarTopicSenders) {
        if (CollectionUtils.isEmpty(pulsarTopicSenders)) {
            return null;
        }
        if (index < 0) {
            index = Instant.now().getNano();
        }
        return pulsarTopicSenders.get(index % pulsarTopicSenders.size());
    }

    public List<PulsarClient> getPulsarClients(String dbJobId,
            List<MQClusterInfo> mqClusterInfos) {
        if (CollectionUtils.isEmpty(mqClusterInfos)) {
            LOGGER.error("init job[{}] pulsar client fail, empty mqCluster info", dbJobId);
            return null;
        }
        List<PulsarClient> pulsarClients = new ArrayList<>();
        for (MQClusterInfo clusterInfo : mqClusterInfos) {
            if (clusterInfo.isValid()) {
                PulsarClient client = genPulsarClient(dbJobId, clusterInfo.getUrl());
                if (client != null) {
                    pulsarClients.add(client);
                }
                LOGGER.info("job[{}] init pulsar client url={}", dbJobId, clusterInfo.getUrl());
            } else {
                LOGGER.warn("job[{}] init pulsar client url={}, type={} config has error!", dbJobId,
                        clusterInfo.getUrl(), clusterInfo.getMqType());
            }
        }
        return pulsarClients;
    }

    public void destoryPulsarClient() {
        if (pulsarClientMap != null) {
            pulsarClientMap.forEach((k, pulsarClient) -> {
                if (pulsarClient != null) {
                    try {
                        pulsarClient.close();
                    } catch (PulsarClientException e) {
                        LOGGER.warn("job [{}] close pulsar client has error!", dbJobId, e);
                    }
                    LOGGER.info("job [{}] close pulsar client!", dbJobId);
                }
            });
            pulsarClientMap.clear();
        }
    }

    private PulsarClient genPulsarClient(String dbJobId, String clusterUrl) {
        String clientKey = getPulsarClientMapKey(clusterUrl);
        return pulsarClientMap.computeIfAbsent(clientKey, (k) -> {
            PulsarClient client = null;
            try {
                client = PulsarClient.builder().serviceUrl(clusterUrl)
                        .ioThreads(clientIoThreads)
                        .connectionsPerBroker(connectionsPreBroker).build();
            } catch (Exception e) {
                LOGGER.error("init job[{}] pulsar client with url {} is failure", dbJobId, clusterUrl, e);
            }
            return client;
        });
    }

    private String getPulsarClientMapKey(String clusterUrl) {
        return clusterUrl;
    }

    private String getPulsarSenderMapKey(String threadId, String topic) {
        return threadId + "_" + topic;
    }

    @Override
    public String report() {
        return "taskTopicNum:" + pulsarTopicSenderMap.size() + "|pulsarClientNum:" + pulsarClientMap.size();
    }
}
