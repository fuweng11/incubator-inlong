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

package org.apache.inlong.manager.service.resource.sort.tencent.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.PulsarClusterInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.sink.EsSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.EsSinkInfo.EsClusterInfo;
import com.tencent.oceanus.etl.protocol.source.PulsarSourceInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
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
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchSink;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implementation of sort Elasticsearch configuration
 */
@Slf4j
@Service
public class SortEsConfigService extends AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortEsConfigService.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper(); // thread safe

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    public void buildEsConfig(InlongGroupInfo groupInfo, List<ElasticsearchSink> elasticsearchSinkList) {
        if (CollectionUtils.isEmpty(elasticsearchSinkList)) {
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
                groupInfo.getInlongClusterTag(), null, ClusterType.SORT_ES);
        if (CollectionUtils.isEmpty(sortClusters) || StringUtils.isBlank(sortClusters.get(0).getName())) {
            throw new WorkflowListenerException("sort cluster not found for groupId=" + groupInfo.getInlongGroupId());
        }
        String taskName = sortClusters.get(0).getName();
        for (ElasticsearchSink elasticsearchSink : elasticsearchSinkList) {
            log.info("begin to push sort elasticsearch config to zkUrl={}, taskName={}", zkUrl, taskName);
            try {
                DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, elasticsearchSink);
                // Update / add data under dataflow on ZK
                ZkTools.updateDataFlowInfo(flowInfo, taskName, flowInfo.getId(), zkUrl, zkRoot);
                // Add data under clusters on ZK
                ZkTools.addDataFlowToCluster(taskName, flowInfo.getId(), zkUrl, zkRoot);
                String info = "success to push elasticsearch sort config";
                log.info("success to push elasticsearch sort config {}", JSON_MAPPER.writeValueAsString(flowInfo));
            } catch (Exception e) {
                String errMsg = "failed to push elasticsearch sort config: " + e.getMessage();
                LOGGER.error(errMsg, e);
                throw new BusinessException(errMsg);
            }
        }
    }

    /**
     * Get DataFlowInfo for Sort
     */
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, ElasticsearchSink elasticsearchSink)
            throws Exception {
        SourceInfo sourceInfo = getSourceInfo(groupInfo, elasticsearchSink);
        EsSinkInfo esSink = getEsSinkInfo(groupInfo, elasticsearchSink);
        // TransformInfo transformInfo = getTransformInfo(groupInfo, innerEsSink);
        String flowId = elasticsearchSink.getId().toString();
        // Dynamic configuration information,
        // which can be used to specify optional parameter information of source or sink
        // After that, source.tdbank.bid, source.tdbank.tid will be dropped
        // Just stay source.inlong.groupId, source.inlong.streamIdF
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("source.tdbank.bid", elasticsearchSink.getInlongGroupId());
        properties.put("source.tdbank.tid", elasticsearchSink.getInlongStreamId());
        properties.put("source.inlongGroupId", elasticsearchSink.getInlongGroupId());
        properties.put("source.inlongStreamId", elasticsearchSink.getInlongStreamId());
        DataFlowInfo flowInfo = new DataFlowInfo(flowId, sourceInfo, esSink, properties);
        log.info("elasticsearch data flow info: " + JSON_MAPPER.writeValueAsString(flowInfo));

        return flowInfo;
    }

    /**
     * Assembly source information
     */
    private SourceInfo getSourceInfo(InlongGroupInfo groupInfo, ElasticsearchSink elasticsearchSink)
            throws Exception {
        String groupId = elasticsearchSink.getInlongGroupId();
        String streamId = elasticsearchSink.getInlongStreamId();
        InlongStreamEntity stream = streamEntityMapper.selectByIdentifier(groupId, streamId);
        List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                groupInfo.getInlongClusterTag(), null, ClusterType.SORT_ES);
        String taskName = sortClusters.get(0).getName();
        if (taskName == null || StringUtils.isBlank(taskName)) {
            throw new WorkflowListenerException("elasticsearch topo cluster not found for groupId=" + groupId);
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
            List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(elasticsearchSink.getId());
            // List<InnerElasticsearchFieldInfo> fieldList = getElasticsearchFieldFromSink(innerEsSink);
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

            List<PulsarClusterInfo> pulsarClusterInfos = new ArrayList<>();
            pulsarClusters.forEach(pulsarCluster -> {
                // Multiple adminurls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(pulsarCluster.getExtParams());
                String adminUrl = pulsarClusterDTO.getAdminUrl();
                String serviceUrl = pulsarCluster.getUrl();
                pulsarClusterInfos.add(new PulsarClusterInfo(adminUrl, serviceUrl, null, null));
            });

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
            List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(elasticsearchSink.getId());
            // List<InnerElasticsearchFieldInfo> fieldList = getElasticsearchFieldFromSink(innerEsSink);
            try {
                DeserializationInfo deserializationInfo = getDeserializationInfo(stream);
                // Ensure compatibility of old data: if the old subscription exists, use the old one;
                // otherwise, create the subscription according to the new rule
                String subscription = getConsumerGroup(groupInfo, topic, taskName, elasticsearchSink.getId());
                sourceInfo = new PulsarSourceInfo(adminUrl, masterAddress, fullTopic, subscription,
                        deserializationInfo, fieldList.stream().map(f -> {
                            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                                    f.getSourceFieldType().toLowerCase());
                            return new FieldInfo(f.getSourceFieldType(), formatInfo);
                        }).toArray(FieldInfo[]::new), pulsarClusterInfos.toArray(new PulsarClusterInfo[0]), null);
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
    private EsSinkInfo getEsSinkInfo(InlongGroupInfo groupInfo, ElasticsearchSink elasticsearchSink)
            throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        String streamId = elasticsearchSink.getInlongStreamId();
        DataNodeEntity esDataNode = dataNodeEntityMapper.selectByUniqueKey(elasticsearchSink.getDataNodeName(),
                DataNodeType.ELASTICSEARCH);
        if (esDataNode == null) {
            log.error("can not find elasticsearch cluster for {} - {} ", groupId, streamId);
            throw new Exception("can not find elasticsearch cluster");
        }
        String[] esUrl = esDataNode.getUrl().split(":");
        String esAddress = esUrl[0];
        int esPort = Integer.parseInt(esUrl[1]);
        EsSinkInfo.EsClusterInfo esClusterInfo = new EsClusterInfo(esAddress, esPort);
        List<EsSinkInfo.EsClusterInfo> esClusterInfoList = new ArrayList<>();
        esClusterInfoList.add(esClusterInfo);
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(elasticsearchSink.getId());
        return new EsSinkInfo(
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new),
                esDataNode.getUsername(), esDataNode.getToken(), elasticsearchSink.getIndexName(),
                null, String.valueOf(elasticsearchSink.getEsVersion()),
                esClusterInfoList.toArray(new EsClusterInfo[0]));
    }

}
