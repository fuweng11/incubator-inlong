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
import org.apache.inlong.manager.pojo.node.tencent.InnerHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveSinkDTO;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.node.DataNodeOperator;
import org.apache.inlong.manager.service.node.DataNodeOperatorFactory;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
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
    private StreamSinkEntityMapper sinkMapper;
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
        for (SinkInfo sinkInfo : configList) {
            switch (sinkInfo.getSinkType()) {
                case DataNodeType.INNER_HIVE:
                    InnerHiveSinkDTO hiveInfo = InnerHiveSinkDTO.getFromJson(sinkInfo.getExtParams());
                    StreamSinkEntity sink = sinkMapper.selectByPrimaryKey(sinkInfo.getId());
                    InnerHiveDataNodeInfo dataNodeInfo = (InnerHiveDataNodeInfo) getDataNodeInfo(sink.getDataNodeName(),
                            DataNodeType.INNER_HIVE);
                    InnerHiveFullInfo hiveFullInfo = InnerHiveSinkDTO.getInnerHiveTableInfo(hiveInfo, sinkInfo,
                            dataNodeInfo);
                    hiveInfos.add(hiveFullInfo);
                    break;
                case DataNodeType.INNER_CLICKHOUSE:
                default:
                    LOGGER.warn("skip to push sort config for sink id={}, as no sort config info", sinkInfo.getId());
            }
        }
        sortHiveConfigService.buildHiveConfig(groupInfo, hiveInfos);
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
