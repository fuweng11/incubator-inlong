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

package org.apache.inlong.manager.service.resource.sort.tencent.ck;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.sink.ClickHouseSinkInfo;
import com.tencent.oceanus.etl.protocol.source.PulsarSourceInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.ck.InnerClickHouseSink;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

/**
 * Implementation of sort Clickhouse configuration
 */
@Slf4j
@Service
public class SortCkConfigService extends AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortCkConfigService.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private StreamSinkService sinkService;

    public void buildCkConfig(InlongGroupInfo groupInfo, List<InnerClickHouseSink> clickHouseSinkList)
            throws Exception {
        if (CollectionUtils.isEmpty(clickHouseSinkList)) {
            return;
        }
        // Use new protocol for all
        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(groupInfo.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters)) {
            String errMsg = String.format("zk cluster is null for cluster tag=%s", groupInfo.getInlongClusterTag());
            log.error(errMsg);
            throw new BusinessException(errMsg);
        }
        InlongClusterEntity zkCluster = zkClusters.get(0);
        ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());

        String zkUrl = zkCluster.getUrl();
        String zkRoot = getZkRoot(groupInfo.getMqType(), zkClusterDTO);
        List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                groupInfo.getInlongClusterTag(), null, ClusterType.SORT_CK);
        if (CollectionUtils.isEmpty(sortClusters) || StringUtils.isBlank(sortClusters.get(0).getName())) {
            throw new WorkflowListenerException("sort cluster not found for groupId=" + groupInfo.getInlongGroupId());
        }
        String topoName = sortClusters.get(0).getName();
        for (InnerClickHouseSink clickHouseSink : clickHouseSinkList) {
            log.info("begin to push sort ck config to zkUrl={}, ckTopo={}", zkUrl, topoName);
            try {
                DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, clickHouseSink);
                // Update / add data under dataflow on ZK
                ZkTools.updateDataFlowInfo(flowInfo, topoName, flowInfo.getId(), zkUrl, zkRoot);
                // Add data under clusters on ZK
                ZkTools.addDataFlowToCluster(topoName, flowInfo.getId(), zkUrl, zkRoot);
                String info = "success to push clickhouse sort config";
                sinkService.updateStatus(clickHouseSink.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
                log.info("success to push ck sort config {}", JSON_MAPPER.writeValueAsString(flowInfo));
            } catch (Exception e) {
                String errMsg = "failed to push clickhouse sort config: " + e.getMessage();
                LOGGER.error(errMsg, e);
                sinkService.updateStatus(clickHouseSink.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
                throw new BusinessException(errMsg);
            }
        }
    }

    /**
     * Get DataFlowInfo for Sort
     */
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, InnerClickHouseSink clickHouseSink)
            throws Exception {
        SourceInfo sourceInfo = getSourceInfo(groupInfo, clickHouseSink);
        ClickHouseSinkInfo ckSink = getCkSinkInfo(groupInfo, clickHouseSink);

        String flowId = clickHouseSink.getId().toString();
        // Dynamic configuration information,
        // which can be used to specify optional parameter information of source or sink
        // After that, source.tdbank.bid, source.tdbank.tid will be dropped
        // Just stay source.inlong.groupId, source.inlong.streamId
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("source.tdbank.bid", clickHouseSink.getInlongGroupId());
        properties.put("source.tdbank.tid", clickHouseSink.getInlongStreamId());
        properties.put("source.inlongGroupId", clickHouseSink.getInlongGroupId());
        properties.put("source.inlongStreamId", clickHouseSink.getInlongStreamId());
        DataFlowInfo flowInfo = new DataFlowInfo(flowId, sourceInfo, ckSink, properties);
        log.info("click house data flow info: " + JSON_MAPPER.writeValueAsString(flowInfo));

        return flowInfo;
    }

    /**
     * Assembly source information
     */
    private SourceInfo getSourceInfo(InlongGroupInfo groupInfo, InnerClickHouseSink clickHouseSink) throws Exception {
        String groupId = clickHouseSink.getInlongGroupId();
        String streamId = clickHouseSink.getInlongStreamId();
        InlongStreamEntity stream = streamEntityMapper.selectByIdentifier(groupId, streamId);
        List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                groupInfo.getInlongClusterTag(), null, ClusterType.SORT_CK);
        String topoName = sortClusters.get(0).getName();
        if (topoName == null || StringUtils.isBlank(topoName)) {
            throw new WorkflowListenerException("click house topo cluster not found for groupId=" + groupId);
        }
        String mqType = groupInfo.getMqType();
        SourceInfo sourceInfo = null;
        if (MQType.TUBEMQ.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> tubeClusters = clusterMapper.selectByKey(
                    groupInfo.getInlongClusterTag(), null, MQType.TUBEMQ);
            if (CollectionUtils.isEmpty(tubeClusters)) {
                throw new WorkflowListenerException("tube cluster not found for groupId=" + groupId);
            }
            InlongClusterEntity tubeCluster = tubeClusters.get(0);
            Preconditions.checkNotNull(tubeCluster, "tube cluster not found for groupId=" + groupId);
            String masterAddress = tubeCluster.getUrl();
            Preconditions.checkNotNull(masterAddress,
                    "tube cluster [" + tubeCluster.getId() + "] not contains masterAddress");
            DeserializationInfo deserializationInfo = getDeserializationInfo(stream);

            List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
            // The consumer group name is pushed to null, which is constructed by the sort side
            return new TubeSourceInfo(
                    groupInfo.getMqResource(),
                    masterAddress,
                    null,
                    deserializationInfo,
                    fieldList.stream().map(f -> {
                        FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                                f.getSourceFieldType().toLowerCase());
                        return new FieldInfo(f.getSourceFieldType(), formatInfo);
                    }).toArray(FieldInfo[]::new));
        } else if (MQType.PULSAR.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> pulsarClusters = clusterMapper.selectByKey(
                    groupInfo.getInlongClusterTag(), null, MQType.PULSAR);
            if (CollectionUtils.isEmpty(pulsarClusters)) {
                throw new WorkflowListenerException("pulsar cluster not found for groupId=" + groupId);
            }
            InlongClusterEntity pulsarCluster = pulsarClusters.get(0);
            // Multiple adminurls should be configured for pulsar,
            // otherwise all requests will be sent to the same broker
            PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(pulsarCluster.getExtParams());
            String adminUrl = pulsarClusterDTO.getAdminUrl();
            String masterAddress = pulsarCluster.getUrl();
            String tenant = pulsarClusterDTO.getTenant() == null ? InlongConstants.DEFAULT_PULSAR_TENANT
                    : pulsarClusterDTO.getTenant();
            String namespace = groupInfo.getMqResource();
            String topic = stream.getMqResource();
            // Full path of topic in pulsar
            String fullTopic = "persistent://" + tenant + "/" + namespace + "/" + topic;
            List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
            try {
                DeserializationInfo deserializationInfo = getDeserializationInfo(stream);
                // Ensure compatibility of old data: if the old subscription exists, use the old one;
                // otherwise, create the subscription according to the new rule
                String subscription = getConsumerGroup(groupId, streamId, topic, topoName, MQType.PULSAR);
                sourceInfo = new PulsarSourceInfo(adminUrl, masterAddress, fullTopic, subscription,
                        deserializationInfo, fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new));
            } catch (Exception e) {
                LOGGER.error("get pulsar information failed", e);
                throw new WorkflowListenerException("get pulsar admin failed, reason: " + e.getMessage());
            }
        }
        return sourceInfo;
    }

    /**
     * Assembling sink information
     */
    private ClickHouseSinkInfo getCkSinkInfo(InlongGroupInfo groupInfo, InnerClickHouseSink clickHouseSink)
            throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        String streamId = clickHouseSink.getInlongStreamId();
        DataNodeEntity ckCluster = dataNodeEntityMapper.selectByUniqueKey(clickHouseSink.getDataNodeName(),
                DataNodeType.INNER_CK);
        if (ckCluster == null) {
            log.error("can not find click house cluster for {} - {} ", groupId, streamId);
            throw new Exception("can not find click house cluster");
        }
        ClickHouseSinkInfo.PartitionStrategy partition = ClickHouseSinkInfo.PartitionStrategy.HASH;
        if (clickHouseSink.getIsDistribute() == 1) {
            String strategy = clickHouseSink.getPartitionStrategy();
            /*if (strategy.equalsIgnoreCase("HASH")) {
                partition = ClickHouseSinkInfo.PartitionStrategy.HASH;
            } else */
            if (strategy.equalsIgnoreCase(TencentConstants.STRATEGY_BALANVE)) {
                partition = ClickHouseSinkInfo.PartitionStrategy.BALANCE;
            } else if (strategy.equalsIgnoreCase(TencentConstants.STRATEGY_RANDOM)) {
                partition = ClickHouseSinkInfo.PartitionStrategy.RANDOM;
            }
        }
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
        return new ClickHouseSinkInfo(
                "clickhouse://" + ckCluster.getUrl(), clickHouseSink.getDbName(),
                clickHouseSink.getTableName(), ckCluster.getUsername(), ckCluster.getToken(),
                clickHouseSink.getIsDistribute() != 0, partition,
                clickHouseSink.getPartitionFields() == null ? "" : clickHouseSink.getPartitionFields(),
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new),
                new String[0],
                clickHouseSink.getFlushInterval(),
                clickHouseSink.getPackageSize(),
                clickHouseSink.getRetryTime());
    }

}
