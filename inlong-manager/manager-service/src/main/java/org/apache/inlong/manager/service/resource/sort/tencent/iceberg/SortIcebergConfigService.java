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

package org.apache.inlong.manager.service.resource.sort.tencent.iceberg;

import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.sink.IcebergSinkInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSink;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.QueryIcebergTableResponse;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

import static org.apache.inlong.manager.common.util.JsonUtils.OBJECT_MAPPER;

/**
 * Implementation of sort Iceberg configuration
 */
@Slf4j
@Service
public class SortIcebergConfigService extends AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortIcebergConfigService.class);

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;
    @Autowired
    private IcebergBaseOptService icebergBaseOptService;

    public void buildIcebergConfig(InlongGroupInfo groupInfo, List<InnerIcebergSink> icebergSinkList)
            throws Exception {
        if (CollectionUtils.isEmpty(icebergSinkList)) {
            return;
        }
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
        for (InnerIcebergSink icebergSink : icebergSinkList) {
            QueryIcebergTableResponse tableDetail = icebergBaseOptService.getTableDetail(icebergSink);
            log.info("iceberg table info: {}", OBJECT_MAPPER.writeValueAsString(tableDetail));

            Integer icebergId = icebergSink.getId();
            String dbName = icebergSink.getDbName();
            String tableName = icebergSink.getTableName();
            if (tableDetail.getCode() != 0) {
                log.info("table [{}.{}] not ready, skip push config for iceberg={}", dbName, tableName, icebergId);
                continue;
            }
            // get sort task name for sink
            String sortClusterName = getSortTaskName(groupInfo.getInlongGroupId(), groupInfo.getInlongClusterTag(),
                    icebergSink.getId(), ClusterType.SORT_ICEBERG);
            // table not exists, push config to zk
            log.info("begin to push iceberg config [{}] to zkUrl={}, icebergTopo={}", icebergId, zkUrl,
                    sortClusterName);
            DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, icebergSink, tableDetail, sortClusterName);
            ZkTools.updateDataFlowInfo(flowInfo, sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
            ZkTools.addDataFlowToCluster(sortClusterName, flowInfo.getId(), zkUrl, zkRoot);

            log.info("success to push iceberg sort config {}", OBJECT_MAPPER.writeValueAsString(flowInfo));
        }
    }

    /**
     * Get DataFlowInfo for Sort
     */
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, InnerIcebergSink icebergSink,
            QueryIcebergTableResponse tableDetail, String sortClusterName) throws Exception {
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(icebergSink.getId());
        SourceInfo sourceInfo = getSourceInfo(groupInfo, icebergSink, sortClusterName, fieldList);
        IcebergSinkInfo icebergSinkInfo = getIcebergSinkInfo(icebergSink, tableDetail);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("source.tdbank.bid", groupInfo.getInlongGroupId());
        properties.put("source.tdbank.tid", icebergSink.getInlongStreamId());
        properties.put("source.inlongGroupId", groupInfo.getInlongGroupId());
        properties.put("source.inlongStreamId", icebergSink.getInlongStreamId());
        DataFlowInfo flowInfo = new DataFlowInfo(icebergSink.getId().toString(), sourceInfo, icebergSinkInfo,
                properties);
        log.info("iceberg data flow info: " + OBJECT_MAPPER.writeValueAsString(flowInfo));

        return flowInfo;
    }

    /**
     * get sink info
     */
    private IcebergSinkInfo getIcebergSinkInfo(InnerIcebergSink icebergSink, QueryIcebergTableResponse tableDetail) {
        QueryIcebergTableResponse.TableStructure structure = tableDetail.getData();
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(icebergSink.getId());
        return new IcebergSinkInfo(
                icebergSink.getDbName(),
                icebergSink.getTableName(),
                tableDetail.getData().getLocation(),
                structure.getTableInfosForTdsort().getSchemaAsJson(),
                structure.getTableInfosForTdsort().getPartitionSpecAsJson(),
                icebergSink.getCreator(),
                structure.getProperties(),
                structure.getTableInfosForTdsort().getHadoopConfProps(),
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new),
                icebergSink.getDataEncoding());
    }
}
