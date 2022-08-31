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

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.tencent.ck.InnerClickHouseSink;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerBaseHiveSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.node.DataNodeOperator;
import org.apache.inlong.manager.service.node.DataNodeOperatorFactory;
import org.apache.inlong.manager.service.resource.sort.tencent.ck.SortCkConfigService;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Inner Sort config operator, used to create a Sort config for the InlongGroup with ZK enabled.
 */
@Service
public class InnerSortConfigOperator implements SortConfigOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerSortConfigOperator.class);

    @Autowired
    SortHiveConfigService sortHiveConfigService;
    @Autowired
    SortCkConfigService sortCkConfigService;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private StreamSinkService streamSinkService;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private DataNodeOperatorFactory operatorFactory;

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
        List<String> streamIds = streamInfos.stream().map(InlongStreamInfo::getInlongStreamId)
                .collect(Collectors.toList());
        List<SinkInfo> configList = sinkMapper.selectAllConfig(groupId, streamIds);
        List<InnerHiveFullInfo> hiveInfos = new ArrayList<>();
        List<InnerClickHouseSink> clickHouseSinkList = new ArrayList<>();
        for (SinkInfo sinkInfo : configList) {
            switch (sinkInfo.getSinkType()) {
                case DataNodeType.INNER_HIVE:
                case DataNodeType.INNER_THIVE:
                    InnerBaseHiveSinkDTO hiveInfo = InnerBaseHiveSinkDTO.getFromJson(sinkInfo.getExtParams());
                    StreamSinkEntity sink = sinkMapper.selectByPrimaryKey(sinkInfo.getId());
                    InnerBaseHiveDataNodeInfo dataNodeInfo = (InnerBaseHiveDataNodeInfo) getDataNodeInfo(
                            sink.getDataNodeName(),
                            sink.getSinkType());
                    InnerHiveFullInfo hiveFullInfo = InnerBaseHiveSinkDTO.getHiveFullInfo(hiveInfo, sinkInfo,
                            dataNodeInfo);
                    hiveInfos.add(hiveFullInfo);
                    break;
                case DataNodeType.INNER_CK:
                    InnerClickHouseSink clickHouseSink = (InnerClickHouseSink) streamSinkService.get(sinkInfo.getId());
                    clickHouseSinkList.add(clickHouseSink);
                    break;
                default:
                    LOGGER.warn("skip to push sort config for sink id={}, as no sort config info", sinkInfo.getId());
            }
        }
        sortHiveConfigService.buildHiveConfig(groupInfo, hiveInfos);
        sortCkConfigService.buildCkConfig(groupInfo, clickHouseSinkList);
    }

    private DataNodeInfo getDataNodeInfo(String dataNodeName, String dataNodeType) {
        DataNodeEntity entity = dataNodeEntityMapper.selectByNameAndType(dataNodeName, dataNodeType);
        if (entity == null) {
            LOGGER.error("data node not found by name={}, type={}", dataNodeName, dataNodeType);
            throw new BusinessException("data node not found");
        }
        DataNodeOperator dataNodeOperator = operatorFactory.getInstance(dataNodeType);
        DataNodeInfo dataNodeInfo = dataNodeOperator.getFromEntity(entity);
        LOGGER.debug("success to get data node info by name={}, type={}", dataNodeName, dataNodeType);
        return dataNodeInfo;
    }

}
