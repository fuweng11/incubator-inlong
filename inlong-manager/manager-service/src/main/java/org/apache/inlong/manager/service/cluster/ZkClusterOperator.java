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

package org.apache.inlong.manager.service.cluster;

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterInfo;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Zk cluster operator.
 */
@Slf4j
@Service
public class ZkClusterOperator extends AbstractClusterOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClusterOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String clusterType) {
        return getClusterType().equals(clusterType);
    }

    @Override
    public String getClusterType() {
        return ClusterType.ZOOKEEPER;
    }

    @Override
    protected void setTargetEntity(ClusterRequest request, InlongClusterEntity targetEntity) {
        ZkClusterRequest zkClusterRequest = (ZkClusterRequest) request;
        CommonBeanUtils.copyProperties(zkClusterRequest, targetEntity, true);
        try {
            ZkClusterDTO dto = ZkClusterDTO.getFromRequest(zkClusterRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.info("success to set entity for zk cluster");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    @Override
    public ClusterInfo getFromEntity(InlongClusterEntity entity) {
        ZkClusterInfo clusterInfo = new ZkClusterInfo();
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        ZkClusterDTO dto = ZkClusterDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, clusterInfo, true);
        CommonBeanUtils.copyProperties(dto, clusterInfo, true);
        return clusterInfo;
    }

    @Override
    public Object getClusterInfo(InlongClusterEntity entity) {
        ZkClusterInfo zkClusterInfo = (ZkClusterInfo) this.getFromEntity(entity);
        Map<String, String> map = new HashMap<>();
        map.put("serverUrl", zkClusterInfo.getUrl());
        return map;
    }
}
