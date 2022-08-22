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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.sink.ClickHouseSinkInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.ck.InnerClickHouseSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Implementation of sort Clickhouse configuration
 */
@Slf4j
@Service
public class SortCkConfigService extends AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortCkConfigService.class);

    private static final Gson GSON = new GsonBuilder().create(); // thread safe
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper(); // thread safe

    @Value("${cluster.clickhouse.topo:oceanus_etl_ck_test_cluster}")
    private String clusterClickHouseTopo;

    @Autowired
    private InlongClusterEntityMapper inlongClusterEntityMapper;
    @Autowired
    private InlongStreamService inlongStreamService;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    public void buildCkConfig(InlongGroupInfo groupInfo, List<InnerClickHouseSink> clickHouseSinkList)
            throws Exception {
        // Use new protocol for all
        List<InlongClusterEntity> zkClusters = inlongClusterEntityMapper.selectByKey(groupInfo.getInlongClusterTag(),
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
        String topoName = clusterClickHouseTopo;
        for (InnerClickHouseSink clickHouseSink : clickHouseSinkList) {
            log.info("begin to push sort ck config to zkUrl={}, ckTopo={}", zkUrl, topoName);

            DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, clickHouseSink);
            // Update / add data under dataflow on ZK
            ZkTools.updateDataFlowInfo(flowInfo, topoName, flowInfo.getId(), zkUrl, zkRoot);
            // Add data under clusters on ZK
            ZkTools.addDataFlowToCluster(topoName, flowInfo.getId(), zkUrl, zkRoot);

            log.info("success to push ck sort config {}", JSON_MAPPER.writeValueAsString(flowInfo));
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
        DataFlowInfo flowInfo = new DataFlowInfo(flowId, sourceInfo, ckSink);
        log.info("click house data flow info: " + JSON_MAPPER.writeValueAsString(flowInfo));

        return flowInfo;
    }

    /**
     * Assembly source information
     */
    private SourceInfo getSourceInfo(InlongGroupInfo groupInfo, InnerClickHouseSink clickHouseSink) throws Exception {
        String groupId = clickHouseSink.getInlongGroupId();
        String streamId = clickHouseSink.getInlongStreamId();
        InlongStreamInfo stream = inlongStreamService.get(groupId, streamId);
        if (!TencentConstants.DATA_TYPE_CSV.equalsIgnoreCase(stream.getDataType())) {
            log.error("click house only support CSV as message");
            throw new Exception("click house only support CSV as message");
        }

        String mqType = groupInfo.getMqType();
        if (!MQType.TUBEMQ.equalsIgnoreCase(mqType)) {
            log.error("click house only support TUBE as source");
            throw new Exception("click house only support TUBE as source");
        }

        InlongClusterEntity cluster = inlongClusterEntityMapper.selectByKey(
                groupInfo.getInlongClusterTag(), null, MQType.TUBEMQ).get(0);
        Preconditions.checkNotNull(cluster, "tube cluster not found for groupId=" + groupId);
        String masterAddress = cluster.getUrl();
        Preconditions.checkNotNull(masterAddress, "tube cluster [" + cluster.getId() + "] not contains masterAddress");
        char c = (char) Integer.parseInt(stream.getDataSeparator());
        DeserializationInfo deserializationInfo = new TDMsgCsvDeserializationInfo(streamId, c);

        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
        // The consumer group name is pushed to null, which is constructed by the sort side
        return new TubeSourceInfo(
                groupInfo.getMqResource(),
                masterAddress,
                null,
                deserializationInfo,
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new));
    }

    /**
     * Assembling sink information
     */
    private ClickHouseSinkInfo getCkSinkInfo(InlongGroupInfo groupInfo, InnerClickHouseSink clickHouseSink)
            throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        String streamId = clickHouseSink.getInlongStreamId();
        DataNodeEntity ckCluster = dataNodeEntityMapper.selectByNameAndType(clickHouseSink.getDataNodeName(),
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
