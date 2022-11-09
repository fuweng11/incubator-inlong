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

package org.apache.inlong.manager.service.resource.queue.pulsar;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.consume.InlongConsumeService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Operator for create Pulsar Tenant, Namespace, Topic and Subscription
 */
@Slf4j
@Service
public class PulsarResourceOperator implements QueueResourceOperator {

    /**
     * The name rule for Pulsar subscription: clusterTag_namespace_topic_sinkId_consumer_group
     */
    private static final String PULSAR_SUBSCRIPTION = "%s_%s_%s_%s_consumer_group";

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongConsumeService consumeService;
    @Autowired
    private PulsarOperator pulsarOperator;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;

    @Override
    public boolean accept(String mqType) {
        return MQType.PULSAR.equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType);
    }

    @Override
    public void createQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(operator, "operator cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to create pulsar resource for groupId={}", groupId);

        // get pulsar cluster via the inlong cluster tag from the inlong group
        String clusterTag = groupInfo.getInlongClusterTag();
        List<PulsarClusterInfo> pulsarClusters =
                clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR).stream()
                        .map(clusterInfo -> (PulsarClusterInfo) clusterInfo)
                        .collect(Collectors.toList());
        for (PulsarClusterInfo pulsarCluster : pulsarClusters) {
            try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
                String clusterName = pulsarCluster.getName();
                // create pulsar tenant and namespace
                String tenant = pulsarCluster.getTenant();
                if (StringUtils.isEmpty(tenant)) {
                    tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
                }
                String namespace = groupInfo.getMqResource();
                InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
                // if the group was not successful, need create tenant and namespace
                if (!Objects.equal(GroupStatus.CONFIG_SUCCESSFUL.getCode(), groupInfo.getStatus())) {
                    pulsarOperator.createTenant(pulsarAdmin, tenant);
                    log.info("success to create pulsar tenant for groupId={}, tenant={}, cluster={}",
                            groupId, tenant, clusterName);
                    pulsarOperator.createNamespace(pulsarAdmin, pulsarInfo, tenant, namespace);
                    log.info("success to create pulsar namespace for groupId={}, namespace={}, cluster={}",
                            groupId, namespace, clusterName);
                }

                // create pulsar topic - each Inlong Stream corresponds to a Pulsar topic
                List<InlongStreamBriefInfo> streamInfoList = streamService.getTopicList(groupId);
                if (streamInfoList == null || streamInfoList.isEmpty()) {
                    log.warn("skip to create pulsar topic and subscription as no streams for groupId={}, cluster={}",
                            groupId, clusterName);
                    return;
                }
                // create pulsar topic and subscription
                for (InlongStreamBriefInfo stream : streamInfoList) {
                    this.createTopic(pulsarInfo, pulsarCluster, stream.getMqResource());
                    this.createSubscription(pulsarInfo, pulsarCluster, stream.getMqResource(),
                            stream.getInlongStreamId());
                }
            } catch (Exception e) {
                String msg = String.format("failed to create pulsar resource for groupId=%s, cluster=%s", groupId,
                        pulsarCluster.toString());
                log.error(msg, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }
        log.info("success to create pulsar resource for groupId={}", groupId);
    }

    @Override
    public void deleteQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to delete pulsar resource for groupId={}", groupId);

        List<PulsarClusterInfo> pulsarClusters =
                clusterService.listByTagAndType(groupInfo.getInlongClusterTag(), ClusterType.PULSAR).stream()
                        .map(clusterInfo -> (PulsarClusterInfo) clusterInfo)
                        .collect(Collectors.toList());
        for (PulsarClusterInfo clusterInfo : pulsarClusters) {
            String clusterName = clusterInfo.getName();
            try {
                List<InlongStreamBriefInfo> streamInfoList = streamService.getTopicList(groupId);
                if (streamInfoList == null || streamInfoList.isEmpty()) {
                    log.warn("skip to create pulsar topic and subscription as no streams for groupId={}, cluster={}",
                            groupId, clusterName);
                    return;
                }
                for (InlongStreamBriefInfo streamInfo : streamInfoList) {
                    this.deletePulsarTopic(groupInfo, clusterInfo, streamInfo.getMqResource());
                }
            } catch (Exception e) {
                log.error("failed to delete pulsar resource for groupId={}, cluster={}", groupId, clusterName, e);
                throw new WorkflowListenerException("failed to delete pulsar resource: " + e.getMessage());
            }
        }
        log.info("success to delete pulsar resource for groupId={}", groupId);
    }

    @Override
    public void createQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(streamInfo, "inlong stream info cannot be null");
        Preconditions.checkNotNull(operator, "operator cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        log.info("begin to create pulsar resource for groupId={}, streamId={}", groupId, streamId);

        List<PulsarClusterInfo> pulsarClusters =
                clusterService.listByTagAndType(groupInfo.getInlongClusterTag(), ClusterType.PULSAR).stream()
                        .map(clusterInfo -> (PulsarClusterInfo) clusterInfo)
                        .collect(Collectors.toList());
        for (PulsarClusterInfo pulsarCluster : pulsarClusters) {
            try {
                // create pulsar topic and subscription
                this.createTopic((InlongPulsarInfo) groupInfo, pulsarCluster, streamInfo.getMqResource());
                this.createSubscription((InlongPulsarInfo) groupInfo, pulsarCluster,
                        streamInfo.getMqResource(), streamId);
            } catch (Exception e) {
                String msg = String.format("failed to create pulsar topic for groupId=%s, streamId=%s, cluster=%s",
                        groupId, streamId,pulsarCluster.getName());
                log.error(msg, e);
                throw new WorkflowListenerException(msg + ": " + e.getMessage());
            }
        }

        log.info("success to create pulsar resource for groupId={}, streamId={}", groupId, streamId);
    }

    @Override
    public void deleteQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        Preconditions.checkNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.checkNotNull(streamInfo, "inlong stream info cannot be null");

        String groupId = streamInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        log.info("begin to delete pulsar resource for groupId={} streamId={}", groupId, streamId);

        List<PulsarClusterInfo> pulsarClusters =
                clusterService.listByTagAndType(groupInfo.getInlongClusterTag(), ClusterType.PULSAR).stream()
                        .map(clusterInfo -> (PulsarClusterInfo) clusterInfo)
                        .collect(Collectors.toList());
        for (PulsarClusterInfo clusterInfo : pulsarClusters) {
            String clusterName = clusterInfo.getName();
            try {
                this.deletePulsarTopic(groupInfo, clusterInfo, streamInfo.getMqResource());
                log.info("success to delete pulsar topic for groupId={}, streamId={}, cluster={}",
                        groupId, streamId, clusterName);
            } catch (Exception e) {
                String msg = String.format("failed to delete pulsar topic for groupId=%s, streamId=%s, cluster=%s",
                        groupId, streamId, clusterName);
                log.error(msg, e);
                throw new WorkflowListenerException(msg);
            }
        }
        log.info("success to delete pulsar resource for groupId={}, streamId={}", groupId, streamId);
    }

    /**
     * Create Pulsar Topic and Subscription, and save the consumer group info.
     */
    private void createTopic(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster, String topicName)
            throws Exception {
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
            String tenant = pulsarCluster.getTenant();
            if (StringUtils.isEmpty(tenant)) {
                tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
            }
            String namespace = pulsarInfo.getMqResource();
            PulsarTopicInfo topicInfo = PulsarTopicInfo.builder()
                    .tenant(tenant)
                    .namespace(namespace)
                    .topicName(topicName)
                    .queueModule(pulsarInfo.getQueueModule())
                    .numPartitions(pulsarInfo.getPartitionNum())
                    .build();
            pulsarOperator.createTopic(pulsarAdmin, topicInfo);
        }
    }

    /**
     * Create Pulsar Subscription, and save the consumer group info.
     */
    private void createSubscription(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster, String topicName,
            String streamId) throws Exception {
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
            String tenant = pulsarCluster.getTenant();
            if (StringUtils.isEmpty(tenant)) {
                tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
            }
            String namespace = pulsarInfo.getMqResource();
            String fullTopicName = tenant + "/" + namespace + "/" + topicName;
            boolean exist = pulsarOperator.topicExists(pulsarAdmin, tenant, namespace, topicName,
                    InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(pulsarInfo.getQueueModule()));
            if (!exist) {
                String serviceUrl = pulsarCluster.getAdminUrl();
                log.error("topic={} not exists in {}", fullTopicName, serviceUrl);
                throw new WorkflowListenerException("topic=" + fullTopicName + " not exists in " + serviceUrl);
            }

            // create subscription for all sinks
            String groupId = pulsarInfo.getInlongGroupId();
            List<StreamSink> streamSinks = sinkService.listSink(groupId, streamId);
            if (CollectionUtils.isEmpty(streamSinks)) {
                log.warn("no need to create subs, as no sink exists for groupId={}, streamId={}", groupId, streamId);
                return;
            }

            // subscription naming rules: clusterTag_topicName_sinkId_consumer_group
            String clusterTag = pulsarInfo.getInlongClusterTag();
            for (StreamSink sink : streamSinks) {
                String subs = getPulsarSubscription(pulsarInfo, clusterTag, sink, topicName);
                pulsarOperator.createSubscription(pulsarAdmin, fullTopicName, pulsarInfo.getQueueModule(), subs);
                log.info("success to create subs={} for groupId={}, topic={}", subs, groupId, fullTopicName);

                // insert the consumer group info into the inlong_consume table
                Integer id = consumeService.saveBySystem(pulsarInfo, topicName, subs);
                log.info("success to save inlong consume [{}] for subs={}, groupId={}, topic={}",
                        id, subs, groupId, topicName);
            }
        }
    }

    /**
     * Delete Pulsar Topic and Subscription, and delete the consumer group info.
     * TODO delete Subscription and InlongConsume info
     */
    private void deletePulsarTopic(InlongGroupInfo groupInfo, PulsarClusterInfo pulsarCluster, String topicName)
            throws Exception {
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
            String tenant = pulsarCluster.getTenant();
            if (StringUtils.isEmpty(tenant)) {
                tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
            }
            String namespace = groupInfo.getMqResource();
            PulsarTopicInfo topicInfo = PulsarTopicInfo.builder()
                    .tenant(tenant)
                    .namespace(namespace)
                    .topicName(topicName)
                    .build();
            pulsarOperator.forceDeleteTopic(pulsarAdmin, topicInfo);
        }
    }

    private String getPulsarSubscription(InlongGroupInfo groupInfo, String clusterTag, StreamSink sink,
            String topicName) {
        String sortTaskType;
        switch (sink.getSinkType()) {
            case SinkType.INNER_THIVE:
                sortTaskType = ClusterType.SORT_THIVE;
                break;
            case SinkType.INNER_HIVE:
                sortTaskType = ClusterType.SORT_HIVE;
                break;
            case SinkType.INNER_CK:
                sortTaskType = ClusterType.SORT_CK;
                break;
            default:
                return String.format(PULSAR_SUBSCRIPTION, clusterTag, groupInfo.getMqResource(), topicName,
                        sink.getId());
        }
        List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                clusterTag, null, sortTaskType);
        if (CollectionUtils.isEmpty(sortClusters) || StringUtils.isBlank(sortClusters.get(0).getName())) {
            throw new WorkflowListenerException("sort cluster not found for groupId=" + sink.getInlongGroupId());
        }
        String taskName = sortClusters.get(0).getName();
        return String.format(PULSAR_SUBSCRIPTION, taskName, groupInfo.getMqResource(), topicName,
                sink.getId());
    }

}
