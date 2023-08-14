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

package org.apache.inlong.manager.service.resource.queue.tubemq;

import com.google.common.base.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.consume.InlongConsumeService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Operator for create TubeMQ Topic and ConsumerGroup
 */
@Slf4j
@Service
public class TubeMQQueueResourceOperator implements QueueResourceOperator {

    public static final String TUBE_CONSUME_GROUP = "%s_%s_%s_consumer_group";

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongConsumeService consumeService;
    @Autowired
    private TubeMQOperator tubeMQOperator;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private SortHiveConfigService sortConfigService;
    @Autowired
    private InlongConsumeEntityMapper consumeEntityMapper;

    @Override
    public boolean accept(String mqType) {
        return MQType.TUBEMQ.equals(mqType);
    }

    @Override
    public void createQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");
        Preconditions.expectNotBlank(operator, ErrorCodeEnum.INVALID_PARAMETER, "operator cannot be null");

        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to create pulsar resource for groupId={}", groupId);

        // if the group was successful, no need re-create topic and consumer group
        if (Objects.equal(GroupStatus.CONFIG_SUCCESSFUL.getCode(), groupInfo.getStatus())) {
            log.info("skip to create tubemq resource as the status of groupId={} was successful", groupId);
        }

        try {
            // 1. create tubemq topic
            String clusterTag = groupInfo.getInlongClusterTag();
            TubeClusterInfo tubeCluster = (TubeClusterInfo) clusterService.getOne(clusterTag, null, ClusterType.TUBEMQ);
            String topicName = groupInfo.getMqResource();
            tubeMQOperator.createTopic(tubeCluster, topicName, operator);
            log.info("success to create tubemq topic for groupId={}", groupId);
            log.info("success to create tubemq resource for groupId={}, cluster={}", groupId, tubeCluster);
        } catch (Exception e) {
            log.error("failed to create tubemq resource for groupId=" + groupId, e);
            throw new WorkflowListenerException("failed to create tubemq resource: " + e.getMessage());
        }
    }

    @Override
    public void deleteQueueForGroup(InlongGroupInfo groupInfo, String operator) {
        // currently, not support delete tubemq resource for group
    }

    @Override
    public void createQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        // currently, not support create tubemq resource for stream
        String groupId = groupInfo.getInlongGroupId();
        String streamId = streamInfo.getInlongStreamId();
        List<StreamSink> streamSinks = sinkService.listSink(groupId, streamId);
        if (CollectionUtils.isEmpty(streamSinks)) {
            log.warn("no need to create subs, as no sink exists for groupId={}, streamId={}", groupId, streamId);
            return;
        }
        for (StreamSink sink : streamSinks) {
            String topicName = groupInfo.getMqResource();
            String consumeGroup = getTubeConsumerGroup(groupInfo, sink, topicName);
            TubeClusterInfo tubeCluster = (TubeClusterInfo) clusterService.getOne(groupInfo.getInlongClusterTag(), null,
                    ClusterType.TUBEMQ);
            tubeMQOperator.createConsumerGroup(tubeCluster, topicName, consumeGroup, operator);
            log.info("success to create tubemq consumer group for groupId={}", groupId);

            // insert the consumer group info
            Integer id = consumeService.saveBySystem(groupInfo, topicName, consumeGroup);
            log.info("success to save inlong consume [{}] for consumerGroup={}, groupId={}, topic={}",
                    id, consumeGroup, groupId, topicName);

            log.info("success to create tubemq resource for groupId={}, cluster={}", groupId, tubeCluster);
        }
    }

    @Override
    public void deleteQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, String operator) {
        // currently, not support delete tubemq resource for stream
    }

    public List<BriefMQMessage> queryLatestMessages(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            Integer messageCount) {
        Preconditions.expectNotNull(groupInfo, "inlong group info cannot be null");

        String clusterTag = groupInfo.getInlongClusterTag();
        TubeClusterInfo tubeCluster = (TubeClusterInfo) clusterService.getOne(clusterTag, null, ClusterType.TUBEMQ);
        String topicName = groupInfo.getMqResource();

        return tubeMQOperator.queryLastMessage(tubeCluster, topicName, messageCount, streamInfo);
    }

    private String getTubeConsumerGroup(InlongGroupInfo groupInfo, StreamSink sink, String topicName) {
        String clusterTag = groupInfo.getInlongClusterTag();
        String sortTaskType;
        switch (sink.getSinkType()) {
            case SinkType.INNER_THIVE:
                sortTaskType = ClusterType.SORT_THIVE;
                break;
            case SinkType.INNER_HIVE:
                sortTaskType = ClusterType.SORT_HIVE;
                break;
            case SinkType.CLICKHOUSE:
                sortTaskType = ClusterType.SORT_CK;
                break;
            case SinkType.INNER_ICEBERG:
                sortTaskType = ClusterType.SORT_ICEBERG;
                break;
            case SinkType.ELASTICSEARCH:
                sortTaskType = ClusterType.SORT_ES;
                break;
            default:
                return String.format(TUBE_CONSUME_GROUP, clusterTag, groupInfo.getMqResource(), topicName);
        }
        // get sort task name for sink
        String sortClusterName = sortConfigService.getSortTaskName(groupInfo.getInlongGroupId(),
                groupInfo.getInlongClusterTag(), sink.getId(), sortTaskType);
        String oldConsumption = String.format(TencentConstants.OLD_SORT_TUBE_GROUP, sortClusterName,
                groupInfo.getInlongGroupId());
        InlongConsumeEntity exist = consumeEntityMapper.selectExists(oldConsumption, topicName,
                groupInfo.getInlongGroupId());
        if (exist != null) {
            return oldConsumption;
        }
        return String.format(TUBE_CONSUME_GROUP, sortClusterName, clusterTag, topicName);
    }
}
