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
import com.tencent.oceanus.etl.protocol.sink.ClickHouseSinkInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.ck.ClickHouseSink;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
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
    private static final String CLICKHOUSE_JDBC_PREFIX = "jdbc:clickhouse://";

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private StreamSinkService sinkService;

    public void buildCkConfig(InlongGroupInfo groupInfo, List<ClickHouseSink> clickHouseSinkList)
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
        for (ClickHouseSink clickHouseSink : clickHouseSinkList) {
            String sortClusterName = getSortTaskName(groupInfo.getInlongGroupId(), groupInfo.getInlongClusterTag(),
                    clickHouseSink.getId(), ClusterType.SORT_CK);
            log.info("begin to push sort ck config to zkUrl={}, ckTopo={}", zkUrl, sortClusterName);
            try {
                DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, clickHouseSink, sortClusterName);
                // Update / add data under dataflow on ZK
                ZkTools.updateDataFlowInfo(flowInfo, sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
                // Add data under clusters on ZK
                ZkTools.addDataFlowToCluster(sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
                String info = "success to push clickhouse sort config";
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
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, ClickHouseSink clickHouseSink,
            String sortClusterName) throws Exception {
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
        SourceInfo sourceInfo = getSourceInfo(groupInfo, clickHouseSink, sortClusterName, fieldList);
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
     * Assembling sink information
     */
    private ClickHouseSinkInfo getCkSinkInfo(InlongGroupInfo groupInfo, ClickHouseSink clickHouseSink)
            throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        String streamId = clickHouseSink.getInlongStreamId();
        if (StringUtils.isBlank(clickHouseSink.getJdbcUrl())) {
            DataNodeEntity ckCluster = dataNodeEntityMapper.selectByUniqueKey(clickHouseSink.getDataNodeName(),
                    DataNodeType.CLICKHOUSE);
            if (ckCluster == null) {
                log.error("can not find click house cluster for {} - {} ", groupId, streamId);
                throw new Exception("can not find click house cluster");
            }
            clickHouseSink.setJdbcUrl(ckCluster.getUrl());
            clickHouseSink.setUsername(ckCluster.getUsername());
            clickHouseSink.setPassword(ckCluster.getToken());
        }
        ClickHouseSinkInfo.PartitionStrategy partition = ClickHouseSinkInfo.PartitionStrategy.HASH;
        if (clickHouseSink.getIsDistributed() == 1) {
            String strategy = clickHouseSink.getPartitionStrategy();
            /*
             * if (strategy.equalsIgnoreCase("HASH")) { partition = ClickHouseSinkInfo.PartitionStrategy.HASH; } else
             */
            if (strategy.equalsIgnoreCase(TencentConstants.STRATEGY_BALANVE)) {
                partition = ClickHouseSinkInfo.PartitionStrategy.BALANCE;
            } else if (strategy.equalsIgnoreCase(TencentConstants.STRATEGY_RANDOM)) {
                partition = ClickHouseSinkInfo.PartitionStrategy.RANDOM;
            }
        }
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(clickHouseSink.getId());
        String url = clickHouseSink.getJdbcUrl();
        if (url.startsWith(CLICKHOUSE_JDBC_PREFIX)) {
            url = StringUtils.substringAfter(url, CLICKHOUSE_JDBC_PREFIX);
        }
        return new ClickHouseSinkInfo(
                "clickhouse://" + url, clickHouseSink.getDbName(),
                clickHouseSink.getTableName(), clickHouseSink.getUsername(), clickHouseSink.getPassword(),
                clickHouseSink.getIsDistributed() != 0, partition,
                clickHouseSink.getPartitionFields() == null ? "" : clickHouseSink.getPartitionFields(),
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new),
                new String[0],
                clickHouseSink.getFlushInterval(),
                clickHouseSink.getFlushRecord(),
                clickHouseSink.getRetryTimes(),
                clickHouseSink.getDataEncoding());
    }

}
