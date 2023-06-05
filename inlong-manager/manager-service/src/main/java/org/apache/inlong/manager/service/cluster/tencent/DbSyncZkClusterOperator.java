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

package org.apache.inlong.manager.service.cluster.tencent;

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.tencent.dbsynczk.DbSyncZkClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.dbsynczk.DbSyncZkClusterInfo;
import org.apache.inlong.manager.pojo.cluster.tencent.dbsynczk.DbSyncZkClusterRequest;
import org.apache.inlong.manager.service.cluster.AbstractClusterOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DbSyncZkClusterOperator extends AbstractClusterOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbSyncZkClusterOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String clusterType) {
        return getClusterType().equals(clusterType);
    }

    @Override
    public String getClusterType() {
        return ClusterType.DBSYNC_ZK;
    }

    @Override
    protected void setTargetEntity(ClusterRequest request, InlongClusterEntity targetEntity) {
        DbSyncZkClusterRequest dbSyncZkClusterRequest = (DbSyncZkClusterRequest) request;
        CommonBeanUtils.copyProperties(dbSyncZkClusterRequest, targetEntity, true);
        try {
            DbSyncZkClusterDTO dto = DbSyncZkClusterDTO.getFromRequest(dbSyncZkClusterRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("serialize extParams of Elasticsearch Cluster failure: %s", e.getMessage()));
        }
    }

    @Override
    public ClusterInfo getFromEntity(InlongClusterEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        DbSyncZkClusterInfo info = new DbSyncZkClusterInfo();
        CommonBeanUtils.copyProperties(entity, info);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            DbSyncZkClusterDTO dto = DbSyncZkClusterDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, info);
        }
        return info;
    }

    @Override
    public Object getClusterInfo(InlongClusterEntity entity) {
        DbSyncZkClusterInfo dbSyncZkClusterInfo = (DbSyncZkClusterInfo) this.getFromEntity(entity);
        Map<String, String> map = new HashMap<>();
        map.put("url", dbSyncZkClusterInfo.getUrl());
        return map;
    }
}
