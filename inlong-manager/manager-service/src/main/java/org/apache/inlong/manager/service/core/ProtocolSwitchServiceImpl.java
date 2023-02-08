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

package org.apache.inlong.manager.service.core;

import com.tencent.oceanus.etl.configuration.Configuration;
import com.tencent.oceanus.etl.configuration.Constants;
import com.tencent.oceanus.etl.util.ZooKeeperUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.tencent.sp.ProtocolSwitchInfo;
import org.apache.inlong.manager.pojo.tencent.sp.ProtocolSwitchInfo.UnSaveToTmp;
import org.apache.inlong.manager.pojo.tencent.sp.ProtocolSwitchRequest;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.inlong.manager.common.util.JsonUtils.OBJECT_MAPPER;

/**
 * Protocol switch service interface implementation
 */
@Service
public class ProtocolSwitchServiceImpl extends AbstractInnerSortConfigService implements ProtocolSwitchService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolSwitchServiceImpl.class);

    @Autowired
    private InlongGroupEntityMapper groupMapper;

    @Autowired
    private InlongStreamEntityMapper streamMapper;

    @Autowired
    private InlongClusterEntityMapper clusterMapper;

    @Override
    public Boolean switchConfiguration(ProtocolSwitchRequest request) {
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        LOGGER.debug("begin to switch configuration of groupId={}, streamId={}", groupId, streamId);
        Preconditions.expectNotNull(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        Preconditions.expectNotNull(streamId, ErrorCodeEnum.STREAM_ID_IS_EMPTY.getMessage());
        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }

        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            LOGGER.error("inlong stream not found by groupId={}, streamId={}", groupId, streamId);
            throw new BusinessException(ErrorCodeEnum.STREAM_NOT_FOUND);
        }
        String etlZkPath = getEtlZkPath(groupId, streamId);
        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(groupEntity.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters) || StringUtils.isBlank(zkClusters.get(0).getUrl())) {
            throw new BusinessException("sort zk cluster not found for groupId=" + groupId);
        }
        InlongClusterEntity zkCluster = zkClusters.get(0);
        String zkUrl = zkCluster.getUrl();
        String zkRoot = request.getZkRootPath();
        if (StringUtils.isBlank(zkRoot)) {
            ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());
            zkRoot = getZkRoot(groupEntity.getMqType(), zkClusterDTO);
        }

        try {
            LOGGER.info("begin to update etl zk info, zkUrl={}, zkRoot={}, etlZkPath={}",
                    zkUrl, zkRoot, etlZkPath);
            updateZkInfo(request, zkUrl, zkRoot, etlZkPath);
            LOGGER.info("success to update etl zk info, zkUrl={}, zkRoot={}, etlZkPath={}",
                    zkUrl, zkRoot, etlZkPath);
            String usZkPath = getUsZkPath(request.getHdfsTablePath());
            LOGGER.info("begin to update us zk info, usZkPath={}", usZkPath);
            updateZkInfo(request, zkUrl, zkRoot, usZkPath);
            LOGGER.info("success to update us zk info, usZkPath={}", usZkPath);
        } catch (Exception e) {
            LOGGER.error("failed to update etl zk info", e);
            return false;
        }
        return true;
    }

    private void updateZkInfo(ProtocolSwitchRequest request, String zkUrl, String zkRoot, String zkPath) {
        Configuration config = new Configuration();
        config.setString(Constants.ZOOKEEPER_QUORUM, zkUrl);
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);
        CuratorFramework zkClient = ZooKeeperUtils.startCuratorFramework(config);

        try {
            ProtocolSwitchInfo protocolSwitchInfo = new ProtocolSwitchInfo();
            protocolSwitchInfo.setPartitionTime(request.getPartitionTime());
            protocolSwitchInfo.setEtl(new UnSaveToTmp(request.getEtlUnSaveToTmp()));
            protocolSwitchInfo.setTdsort(new UnSaveToTmp(request.getTdsortUnSaveToTmp()));
            byte[] protocolSwitchInfoData = OBJECT_MAPPER.writeValueAsBytes(protocolSwitchInfo);
            if (zkClient.checkExists().forPath(zkPath) == null) {
                ZooKeeperUtils.createRecursive(zkClient, zkPath, protocolSwitchInfoData, CreateMode.PERSISTENT);
            } else {
                zkClient.setData().forPath(zkPath, protocolSwitchInfoData);
            }
            LOGGER.info("success to push protocol switch info={}",
                    OBJECT_MAPPER.writeValueAsString(protocolSwitchInfo));
        } catch (Exception e) {
            String errMsg = String.format("update zk failed for groupId=%s, streamId=%s", request.getInlongGroupId(),
                    request.getInlongStreamId());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }

    }

    private String getEtlZkPath(String groupId, String streamId) {
        return "/sort-flink/switch/etl/" + groupId + "#" + streamId;
    }

    private String getUsZkPath(String hdfsTablePath) {
        return "/sort-flink/switch/us" + hdfsTablePath;
    }
}
