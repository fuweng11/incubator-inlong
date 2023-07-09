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

package org.apache.inlong.manager.service.consume;

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consume.InlongConsumeRequest;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarDTO;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarInfo;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarDTO;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarTopicInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.inlong.manager.service.stream.InlongStreamService;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Inlong consume operator for Pulsar.
 */
@Service
public class ConsumePulsarOperator extends AbstractConsumeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumePulsarOperator.class);

    private static final Integer DLQ_RLQ_ENABLE = 1;
    private static final Integer DLQ__RLQ_DISABLE = 0;
    // Topic prefix for the dead letter queue
    private static final String PREFIX_DLQ = "dlq";
    // Topic prefix for the retry letter queue
    private static final String PREFIX_RLQ = "rlq";

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private PulsarOperator pulsarOperator;

    @Override
    public Boolean accept(String mqType) {
        return getMQType().equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType);
    }

    @Override
    public String getMQType() {
        return MQType.PULSAR;
    }

    @Override
    public void checkTopicInfo(InlongConsumeRequest request) {
        // one inlong stream only has one Pulsar topic,
        // one inlong group may have multiple Pulsar topics.
        String groupId = request.getInlongGroupId();
        InlongGroupTopicInfo topicInfo = groupService.getTopic(groupId);
        Preconditions.expectNotNull(topicInfo, "inlong group not exist for groupId=" + groupId);

        // check the origin topic from request exists
        InlongPulsarTopicInfo pulsarTopic = (InlongPulsarTopicInfo) topicInfo;
        String originTopic = request.getTopic();
        if (originTopic.startsWith("persistent")) {
            originTopic = originTopic.substring(originTopic.lastIndexOf(InlongConstants.SLASH) + 1);
            request.setTopic(originTopic);
        }
        Preconditions.expectTrue(pulsarTopic.getTopics().contains(originTopic),
                "Pulsar topic not exist for " + originTopic);
    }

    @Override
    public InlongConsumeInfo getFromEntity(InlongConsumeEntity entity) {
        Preconditions.expectNotNull(entity, ErrorCodeEnum.CONSUME_NOT_FOUND.getMessage());

        ConsumePulsarInfo consumeInfo = new ConsumePulsarInfo();
        CommonBeanUtils.copyProperties(entity, consumeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            ConsumePulsarDTO dto = ConsumePulsarDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, consumeInfo);
        }
        String groupId = entity.getInlongGroupId();
        InlongGroupInfo groupInfo = groupService.get(groupId);
        String clusterTag = groupInfo.getInlongClusterTag();
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
        Preconditions.expectNotEmpty(clusterInfos, "pulsar cluster not exist for groupId=" + groupId);
        consumeInfo.setClusterInfos(clusterInfos);

        // First get the tenant from the InlongGroup, and then get it from the PulsarCluster.
        String tenant = ((InlongPulsarInfo) groupInfo).getPulsarTenant();
        if (StringUtils.isBlank(tenant)) {
            // If there are multiple Pulsar clusters, take the first one.
            // Note that the tenants in multiple Pulsar clusters must be identical.
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfos.get(0);
            tenant = pulsarCluster.getPulsarTenant();
        }

        consumeInfo.setTopic(getFullPulsarTopic(groupInfo, tenant, entity.getTopic()));
        return consumeInfo;
    }

    @Override
    protected void setTargetEntity(InlongConsumeRequest request, InlongConsumeEntity targetEntity) {
        // prerequisite for RLQ to be turned on: DLQ must be turned on.
        // it means, if DLQ is closed, RLQ cannot exist alone and must be closed.
        ConsumePulsarRequest pulsarRequest = (ConsumePulsarRequest) request;
        boolean dlqEnable = DLQ_RLQ_ENABLE.equals(pulsarRequest.getIsDlq());
        boolean rlqEnable = DLQ_RLQ_ENABLE.equals(pulsarRequest.getIsRlq());
        if (rlqEnable && !dlqEnable) {
            throw new BusinessException(ErrorCodeEnum.PULSAR_DLQ_RLQ_ERROR);
        }

        // TODO when saving, save the enabled DLQ / RLQ into inlong_stream, then create Pulsar topic for them
        // when updating, delete the related DLQ / RLQ info if they were disabled.
        String groupId = targetEntity.getInlongGroupId();
        if (dlqEnable) {
            String dlqTopic = PREFIX_DLQ + "_" + pulsarRequest.getDeadLetterTopic();
            Preconditions.expectTrue(!streamService.exist(groupId, dlqTopic),
                    ErrorCodeEnum.PULSAR_DLQ_DUPLICATED.getMessage());
        } else {
            pulsarRequest.setIsDlq(DLQ__RLQ_DISABLE);
            pulsarRequest.setDeadLetterTopic(null);
            // streamService.logicDeleteDlqOrRlq(groupId, dlqNameOld, operator);
        }
        if (rlqEnable) {
            String rlqTopic = PREFIX_RLQ + "_" + pulsarRequest.getRetryLetterTopic();
            Preconditions.expectTrue(!streamService.exist(groupId, rlqTopic),
                    ErrorCodeEnum.PULSAR_RLQ_DUPLICATED.getMessage());
        } else {
            pulsarRequest.setIsRlq(DLQ__RLQ_DISABLE);
            pulsarRequest.setRetryLetterTopic(null);
            // streamService.logicDeleteDlqOrRlq(groupId, rlqNameOld, operator);
        }

        try {
            ConsumePulsarDTO dto = ConsumePulsarDTO.getFromRequest(pulsarRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CONSUME_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    private String getFullPulsarTopic(InlongGroupInfo groupInfo, String tenant, String topic) {
        if (StringUtils.isEmpty(tenant)) {
            tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
        }
        String namespace = groupInfo.getMqResource();
        return String.format(InlongConstants.PULSAR_TOPIC_FORMAT, tenant, namespace, topic);
    }

    @Override
    public void autoCreateConsumeGroup(InlongConsumeRequest request, InlongGroupEntity groupEntity, String operator) {
        // get pulsar cluster via the inlong cluster tag from the inlong group
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(groupEntity.getInlongClusterTag(),
                ClusterType.PULSAR);
        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        for (ClusterInfo clusterInfo : clusterInfos) {
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
                InlongPulsarDTO pulsarDTO = InlongPulsarDTO.getFromJson(groupEntity.getExtParams());
                String tenant = pulsarDTO.getPulsarTenant();
                if (StringUtils.isBlank(tenant)) {
                    tenant = pulsarCluster.getPulsarTenant();
                }
                String namespace = groupEntity.getMqResource();
                String topicName = streamEntity.getMqResource();
                String fullTopicName = tenant + "/" + namespace + "/" + topicName;
                boolean exist = pulsarOperator.topicExists(pulsarAdmin, tenant, namespace, topicName,
                        InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL.equals(pulsarDTO.getQueueModule()));
                if (!exist) {
                    String serviceUrl = pulsarCluster.getAdminUrl();
                    LOGGER.error("topic={} not exists in {}", fullTopicName, serviceUrl);
                    throw new WorkflowListenerException("topic=" + fullTopicName + " not exists in " + serviceUrl);
                }
                String subs = request.getConsumerGroup();
                pulsarOperator.createSubscription(pulsarAdmin, fullTopicName, pulsarDTO.getQueueModule(), subs);
                LOGGER.info("success to create subs={} for groupId={}, topic={}", subs, groupId, fullTopicName);
            } catch (Exception e) {
                LOGGER.error("create pulsar subscription failed", e);
                throw new WorkflowListenerException(
                        "failed to create pulsar subscription for groupId=" + groupId + ", reason: "
                                + e.getMessage());
            }
        }
    }

}
