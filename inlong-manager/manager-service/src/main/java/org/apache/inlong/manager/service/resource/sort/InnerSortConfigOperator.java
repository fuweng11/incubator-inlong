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

package org.apache.inlong.manager.service.resource.sort;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.StreamStatus;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.ck.ClickHouseSink;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchSink;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerBaseHiveSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.node.DataNodeService;
import org.apache.inlong.manager.service.resource.sort.tencent.ck.SortCkConfigService;
import org.apache.inlong.manager.service.resource.sort.tencent.es.SortEsConfigService;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
import org.apache.inlong.manager.service.resource.sort.tencent.iceberg.SortIcebergConfigService;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Inner Sort config operator, used to create a Sort config for the InlongGroup with ZK enabled.
 */
@Service
public class InnerSortConfigOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerSortConfigOperator.class);

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private DataNodeService dataNodeService;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;

    @Autowired
    private SortCkConfigService ckConfigService;
    @Autowired
    private SortHiveConfigService hiveConfigService;
    @Autowired
    private SortIcebergConfigService icebergConfigService;
    @Autowired
    private SortEsConfigService esConfigService;

    @Override
    public Boolean accept(Integer enableZk) {
        return InlongConstants.ENABLE_ZK.equals(enableZk);
    }

    @Override
    public void buildConfig(InlongGroupInfo groupInfo, List<InlongStreamInfo> streamInfos, boolean isStream)
            throws Exception {
        if (groupInfo == null || CollectionUtils.isEmpty(streamInfos)) {
            LOGGER.warn("group info is null or stream infos is empty, no need to build sort config for disable zk");
            return;
        }
        String groupId = groupInfo.getInlongGroupId();
        Set<String> streamSet = streamMapper.selectByGroupId(groupId).stream()
                .filter(stream -> !StreamStatus.CONFIG_SUCCESSFUL.getCode().equals(stream.getStatus()))
                .map(InlongStreamEntity::getInlongStreamId)
                .collect(Collectors.toSet());
        List<String> streamIds = streamInfos.stream()
                .map(InlongStreamInfo::getInlongStreamId)
                .filter(streamSet::contains).collect(Collectors.toList());
        Map<String, InlongStreamInfo> streamInfoMap = streamInfos.stream()
                .collect(Collectors.toMap(InlongStreamInfo::getInlongStreamId, v -> v));
        if (CollectionUtils.isEmpty(streamIds)) {
            return;
        }

        List<SinkInfo> configList = sinkMapper.selectAllConfig(groupId, streamIds);
        List<InnerHiveFullInfo> hiveInfos = new ArrayList<>();
        List<ClickHouseSink> clickHouseSinkList = new ArrayList<>();
        List<InnerIcebergSink> icebergSinkList = new ArrayList<>();
        List<ElasticsearchSink> elasticsearchSinkList = new ArrayList<>();
        for (SinkInfo sinkInfo : configList) {
            InlongStreamInfo streamInfo = streamInfoMap.get(sinkInfo.getInlongStreamId());
            switch (sinkInfo.getSinkType()) {
                case SinkType.INNER_HIVE:
                case SinkType.INNER_THIVE:
                    InnerBaseHiveSinkDTO hiveInfo = InnerBaseHiveSinkDTO.getFromJson(sinkInfo.getExtParams());
                    StreamSinkEntity sink = sinkMapper.selectByPrimaryKey(sinkInfo.getId());
                    InnerBaseHiveDataNodeInfo dataNodeInfo = (InnerBaseHiveDataNodeInfo) dataNodeService.get(
                            sink.getDataNodeName(), sink.getSinkType());
                    InnerHiveFullInfo hiveFullInfo = InnerBaseHiveSinkDTO.getFullInfo(groupInfo, streamInfo, hiveInfo,
                            sinkInfo, dataNodeInfo);
                    hiveInfos.add(hiveFullInfo);
                    break;
                case SinkType.CLICKHOUSE:
                    ClickHouseSink clickHouseSink = (ClickHouseSink) sinkService.get(sinkInfo.getId());
                    clickHouseSinkList.add(clickHouseSink);
                    break;
                case SinkType.INNER_ICEBERG:
                    InnerIcebergSink icebergSink = (InnerIcebergSink) sinkService.get(sinkInfo.getId());
                    icebergSinkList.add(icebergSink);
                    break;
                case SinkType.ELASTICSEARCH:
                    ElasticsearchSink esSink = (ElasticsearchSink) sinkService.get(sinkInfo.getId());
                    elasticsearchSinkList.add(esSink);
                    break;
                default:
                    LOGGER.warn("skip to push sort config for sink id={}, as no sort config info", sinkInfo.getId());
            }
        }
        hiveConfigService.buildHiveConfig(groupInfo, hiveInfos);
        ckConfigService.buildCkConfig(groupInfo, clickHouseSinkList);
        icebergConfigService.buildIcebergConfig(groupInfo, icebergSinkList);
        esConfigService.buildEsConfig(groupInfo, elasticsearchSinkList);
    }

}
