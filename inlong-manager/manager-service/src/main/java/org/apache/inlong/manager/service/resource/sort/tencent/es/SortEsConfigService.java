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

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchSink;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.sink.EsSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.EsSinkInfo.EsClusterInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
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
        for (ElasticsearchSink elasticsearchSink : elasticsearchSinkList) {
            // get sort task name for sink
            String sortClusterName = getSortTaskName(groupInfo.getInlongGroupId(), groupInfo.getInlongClusterTag(),
                    elasticsearchSink.getId(), ClusterType.SORT_ES);
            log.info("begin to push sort elasticsearch config to zkUrl={}, sortClusterName={}", zkUrl, sortClusterName);
            try {
                DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, elasticsearchSink, sortClusterName);
                // Update / add data under dataflow on ZK
                ZkTools.updateDataFlowInfo(flowInfo, sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
                // Add data under clusters on ZK
                ZkTools.addDataFlowToCluster(sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
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
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, ElasticsearchSink elasticsearchSink,
            String sortClusterName) throws Exception {
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(elasticsearchSink.getId());
        SourceInfo sourceInfo = getSourceInfo(groupInfo, elasticsearchSink, sortClusterName, fieldList);
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
        InlongStreamEntity streamInfo = streamEntityMapper.selectByIdentifier(groupId, streamId);
        return new EsSinkInfo(
                fieldList.stream().map(f -> {
                    FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(f.getFieldType().toLowerCase());
                    return new FieldInfo(f.getFieldName(), formatInfo);
                }).toArray(FieldInfo[]::new),
                esDataNode.getUsername(), esDataNode.getToken(), elasticsearchSink.getIndexName(),
                null, String.valueOf(elasticsearchSink.getEsVersion()),
                esClusterInfoList.toArray(new EsClusterInfo[0]),
                streamInfo.getDataEncoding());
    }

}
