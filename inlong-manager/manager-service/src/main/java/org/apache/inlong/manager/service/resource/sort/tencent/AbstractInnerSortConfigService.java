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

import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.configuration.Constants;
import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgBinlogDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgPbV1DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.KvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongConsumeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

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

    public String getZkRoot(String mqType, ZkClusterDTO zkClusterDTO) {
        Preconditions.checkNotNull(mqType, "mq type cannot be null");
        Preconditions.checkNotNull(zkClusterDTO, "zookeeper cluster cannot be null");

        String zkRoot;
        mqType = mqType.toUpperCase(Locale.ROOT);
        switch (mqType) {
            case MQType.TUBEMQ:
                zkRoot = getZkRootForTube(zkClusterDTO);
                break;
            case MQType.PULSAR:
                zkRoot = getZkRootForPulsar(zkClusterDTO);
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
            default:
                throw new IllegalArgumentException("unsupported data type: " + dataType);
        }

        return deserializationInfo;
    }

    public void deleteSortConfig(StreamSinkEntity sink) throws Exception {
        if (!Objects.equals(sink.getStatus(), SinkStatus.CONFIG_SUCCESSFUL.getCode())) {
            LOGGER.warn("sink is not configured successfully, do not need to delete config in zk");
            return;
        }
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
            case SinkType.INNER_CK:
                topoType = ClusterType.SORT_CK;
                break;
            case SinkType.INNER_ICEBERG:
                topoType = ClusterType.SORT_ICEBERG;
                break;
            case SinkType.ELASTICSEARCH:
                topoType = ClusterType.SORT_ES;
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.MQ_TYPE_NOT_SUPPORTED);
        }
        List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(
                groupInfo.getInlongClusterTag(), null, topoType);
        if (CollectionUtils.isEmpty(sortClusters) || StringUtils.isBlank(sortClusters.get(0).getName())) {
            LOGGER.warn("no matching sort cluster information for groupId=" + groupId);
            return;
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

}
