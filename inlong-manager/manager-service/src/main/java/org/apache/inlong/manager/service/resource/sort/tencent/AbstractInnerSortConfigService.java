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

package org.apache.inlong.manager.service.resource.sort.tencent;

import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.configuration.Constants;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.KafkaClusterInfo;
import com.tencent.oceanus.etl.protocol.PulsarClusterInfo;
import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgBinlogDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgPbV1DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgSeaCubeDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.KvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.source.KafkaSourceInfo;
import com.tencent.oceanus.etl.protocol.source.PulsarSourceInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterDTO;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Default operation of inner sort config.
 */
public class AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInnerSortConfigService.class);

    @Autowired
    private InlongConsumeEntityMapper inlongConsumeMapper;
    @Autowired
    private InlongGroupEntityMapper grouMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;

    public String getZkRoot(String mqType, ZkClusterDTO zkClusterDTO) {
        Preconditions.expectNotNull(mqType, "mq type cannot be null");
        Preconditions.expectNotNull(zkClusterDTO, "zookeeper cluster cannot be null");

        String zkRoot;
        mqType = mqType.toUpperCase(Locale.ROOT);
        switch (mqType) {
            case MQType.TUBEMQ:
                zkRoot = getZkRootForTube(zkClusterDTO);
                break;
            case MQType.PULSAR:
                zkRoot = getZkRootForPulsar(zkClusterDTO);
                break;
            case MQType.KAFKA:
                zkRoot = getZkRootForKafka(zkClusterDTO);
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.MQ_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to get zk root={} for mq type={} from cluster={}", zkRoot, mqType, zkClusterDTO);
        return zkRoot;
    }

    private String getZkRootForPulsar(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getPulsarRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.PULSAR_ROOT_DEFAULT;
        }
        return zkRoot;
    }

    private String getZkRootForTube(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getTubeRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.TUBEMQ_ROOT_DEFAULT;
        }
        return zkRoot;
    }

    private String getZkRootForKafka(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getKafkaRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.KAFKA_ROOT_DEFAULT;
        }
        return zkRoot;
    }

    public String getConsumerGroup(InlongGroupInfo groupInfo, String topic, String taskName, Integer sinkId) {
        String groupId = groupInfo.getInlongGroupId();
        String mqType = groupInfo.getMqType();

        String consumerGroup;
        if (MQType.TUBEMQ.equals(mqType)) {
            consumerGroup = String.format(TencentConstants.SORT_TUBE_GROUP, groupInfo.getInlongClusterTag(), topic);
        } else {
            consumerGroup = String.format(TencentConstants.OLD_SORT_PULSAR_GROUP, taskName, sinkId);
            InlongConsumeEntity exists = inlongConsumeMapper.selectExists(consumerGroup, topic, groupId);
            if (exists == null) {
                consumerGroup = String.format(TencentConstants.SORT_PULSAR_GROUP, taskName, groupInfo.getMqResource(),
                        topic, sinkId);
            }
        }

        LOGGER.debug("success to get consumerGroup={} for groupId={} topic={} taskName={} sinkId={}",
                consumerGroup, groupId, topic, taskName, sinkId);
        return consumerGroup;
    }

    /**
     * Get deserialization info object information
     */
    public DeserializationInfo getDeserializationInfo(InlongStreamEntity streamInfo) {
        String dataType = streamInfo.getDataType();
        Character escape = null;
        DeserializationInfo deserializationInfo;
        if (streamInfo.getDataEscapeChar() != null) {
            escape = streamInfo.getDataEscapeChar().charAt(0);
        }

        String streamId = streamInfo.getInlongStreamId();
        char separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
        switch (dataType) {
            case TencentConstants.DATA_TYPE_BINLOG:
                deserializationInfo = new InlongMsgBinlogDeserializationInfo(streamId);
                break;
            case TencentConstants.DATA_TYPE_CSV:
                // need to delete the first separator? default is false
                deserializationInfo = new InlongMsgCsvDeserializationInfo(streamId, separator, escape, false);
                break;
            case TencentConstants.DATA_TYPE_RAW_CSV:
                deserializationInfo = new CsvDeserializationInfo(separator, escape);
                break;
            case TencentConstants.DATA_TYPE_KV:
                // KV pair separator, which must be the field separator in the data flow
                // TODO should get from the user defined
                char kvSeparator = '&';
                // row separator, which must be a field separator in the data flow
                Character lineSeparator = null;
                // TODO The Sort module need to support
                deserializationInfo = new TDMsgKvDeserializationInfo(streamId, separator, kvSeparator,
                        escape, lineSeparator);
                break;
            case TencentConstants.DATA_TYPE_RAW_KV:
                deserializationInfo = new KvDeserializationInfo(separator, escape);
                break;
            case TencentConstants.DATA_TYPE_INLONG_MSG_V1:
                DeserializationInfo inner = new CsvDeserializationInfo(separator, escape);
                deserializationInfo = new InlongMsgPbV1DeserializationInfo(Constants.CompressionType.GZIP, inner);
                break;
            case TencentConstants.DATA_TYPE_SEA_CUBE:
                deserializationInfo = new InlongMsgSeaCubeDeserializationInfo(streamId);
                break;
            default:
                throw new IllegalArgumentException("unsupported data type: " + dataType);
        }

        return deserializationInfo;
    }

    public void deleteSortConfig(StreamSinkEntity sink) throws Exception {
        InlongGroupEntity groupInfo = grouMapper.selectByGroupId(sink.getInlongGroupId());
        String groupId = sink.getInlongGroupId();
        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(groupInfo.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters) || StringUtils.isBlank(zkClusters.get(0).getUrl())) {
            LOGGER.warn("no matching zk cluster information for groupId=" + groupId);
            return;
        }
        String topoType;
        switch (sink.getSinkType()) {
            case SinkType.INNER_HIVE:
                topoType = ClusterType.SORT_HIVE;
                break;
            case SinkType.INNER_THIVE:
                topoType = ClusterType.SORT_THIVE;
                break;
            case SinkType.CLICKHOUSE:
                topoType = ClusterType.SORT_CK;
                break;
            case SinkType.INNER_ICEBERG:
                topoType = ClusterType.SORT_ICEBERG;
                break;
            case SinkType.ELASTICSEARCH:
                topoType = ClusterType.SORT_ES;
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT);
        }
        String sortClusterName = getSortTaskName(groupInfo.getInlongGroupId(), groupInfo.getInlongClusterTag(),
                sink.getId(),
                topoType);
        InlongClusterEntity zkCluster = zkClusters.get(0);
        ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());
        String zkUrl = zkCluster.getUrl();
        String zkRoot = getZkRoot(groupInfo.getMqType(), zkClusterDTO);
        Integer sinkId = sink.getId();
        LOGGER.info("try to delete sort config from {}, idList={}", zkUrl, sinkId);
        // It could be hive or thive
        ZkTools.removeDataFlowFromCluster(sortClusterName, sinkId.toString(), zkUrl, zkRoot);
        LOGGER.info("success to delete sort config from {}, idList={}", zkUrl, sinkId);
    }

    public String getSortTaskName(String groupId, String clusterTag, Integer sinkId, String sortTaskType) {
        StreamSinkEntity sinkEntity = sinkMapper.selectByPrimaryKey(sinkId);
        String sortClusterName = sinkEntity.getInlongClusterName();
        if (StringUtils.isBlank(sortClusterName)) {
            List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                    clusterTag, null, sortTaskType);
            int minCount = -1;
            for (InlongClusterEntity sortCluster : sortClusters) {
                int isUsedCount = sinkMapper.selectExistByGroupIdAndTaskName(groupId, sortCluster.getName());
                if (minCount < 0 || isUsedCount <= minCount) {
                    minCount = isUsedCount;
                    sortClusterName = sortCluster.getName();
                }
            }
            if (sortClusterName == null || StringUtils.isBlank(sortClusterName)) {
                throw new WorkflowListenerException("topo cluster not found for groupId=" + groupId);
            }
            // save sort task name
            sinkEntity.setInlongClusterName(sortClusterName);
            int rowCount = sinkMapper.updateByIdSelective(sinkEntity);
            if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
                LOGGER.error("sink has already updated with groupId={}, streamId={}, name={}, curVersion={}",
                        sinkEntity.getInlongGroupId(), sinkEntity.getInlongStreamId(), sinkEntity.getSinkName(),
                        sinkEntity.getVersion());
                throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
            }
        }
        return sortClusterName;
    }

    /**
     * Assembly source information
     */
    public SourceInfo getSourceInfo(InlongGroupInfo groupInfo, StreamSink sinkInfo,
            String sortClusterName, List<StreamSinkFieldEntity> fieldList) throws Exception {
        String groupId = sinkInfo.getInlongGroupId();
        String streamId = sinkInfo.getInlongStreamId();
        InlongStreamEntity stream = streamEntityMapper.selectByIdentifier(groupId, streamId);

        String mqType = groupInfo.getMqType();
        SourceInfo sourceInfo = null;
        if (MQType.TUBEMQ.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> tubeClusters = clusterMapper.selectByKey(
                    groupInfo.getInlongClusterTag(), null, MQType.TUBEMQ);
            if (CollectionUtils.isEmpty(tubeClusters)) {
                throw new WorkflowListenerException("tube cluster not found for groupId=" + groupId);
            }
            InlongClusterEntity tubeCluster = tubeClusters.get(0);
            Preconditions.expectNotNull(tubeCluster, "tube cluster not found for groupId=" + groupId);
            String masterAddress = tubeCluster.getUrl();
            Preconditions.expectNotNull(masterAddress,
                    "tube cluster [" + tubeCluster.getId() + "] not contains masterAddress");
            DeserializationInfo deserializationInfo = getDeserializationInfo(stream);
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
                        return new FieldInfo(f.getSourceFieldName(), formatInfo);
                    }).toArray(FieldInfo[]::new),
                    stream.getDataEncoding());
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
                pulsarClusterInfos
                        .add(new PulsarClusterInfo(adminUrl, serviceUrl, pulsarCluster.getName(), null, null));
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
            // List<InnerElasticsearchFieldInfo> fieldList = getElasticsearchFieldFromSink(innerEsSink);
            try {
                DeserializationInfo deserializationInfo = getDeserializationInfo(stream);
                // Ensure compatibility of old data: if the old subscription exists, use the old one;
                // otherwise, create the subscription according to the new rule
                String subscription = getConsumerGroup(groupInfo, topic, sortClusterName, sinkInfo.getId());
                sourceInfo = new PulsarSourceInfo(adminUrl, masterAddress, fullTopic, subscription,
                        deserializationInfo, fieldList.stream().map(f -> {
                            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                                    f.getSourceFieldType().toLowerCase());
                            return new FieldInfo(f.getSourceFieldName(), formatInfo);
                        }).toArray(FieldInfo[]::new), pulsarClusterInfos.toArray(new PulsarClusterInfo[0]), null,
                        stream.getDataEncoding());
            } catch (Exception e) {
                LOGGER.error("get pulsar information failed", e);
                throw new WorkflowListenerException("get pulsar admin failed, reason: " + e.getMessage());
            }
        } else if (MQType.KAFKA.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> kafkaClusters = clusterMapper.selectByKey(
                    groupInfo.getInlongClusterTag(), null, MQType.KAFKA);
            if (CollectionUtils.isEmpty(kafkaClusters)) {
                throw new WorkflowListenerException("kafka cluster not found for groupId=" + groupId);
            }
            List<KafkaClusterInfo> kafkaClusterInfos = new ArrayList<>();
            kafkaClusters.forEach(kafkaCluster -> {
                // Multiple adminurls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                KafkaClusterDTO kafkaClusterDTO = KafkaClusterDTO.getFromJson(kafkaCluster.getExtParams());
                String bootstrapServers = kafkaClusterDTO.getBootstrapServers();
                kafkaClusterInfos.add(new KafkaClusterInfo(bootstrapServers));
            });
            try {
                String topic = stream.getMqResource();
                if (topic.equals(streamId)) {
                    // the default mq resource (stream id) is not sufficient to discriminate different kafka topics
                    topic = String.format(org.apache.inlong.common.constant.Constants.DEFAULT_KAFKA_TOPIC_FORMAT,
                            groupInfo.getMqResource(), stream.getMqResource());
                }
                DeserializationInfo deserializationInfo = getDeserializationInfo(stream);
                sourceInfo = new KafkaSourceInfo(kafkaClusterInfos.toArray(new KafkaClusterInfo[0]), topic, groupId,
                        deserializationInfo,
                        fieldList.stream().map(f -> {
                            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                                    f.getSourceFieldType().toLowerCase());
                            return new FieldInfo(f.getSourceFieldName(), formatInfo);
                        }).toArray(FieldInfo[]::new),
                        stream.getDataEncoding());
            } catch (Exception e) {
                LOGGER.error("get kafka information failed", e);
                throw new WorkflowListenerException("get kafka admin failed, reason: " + e.getMessage());
            }
        }
        return sourceInfo;
    }

}
