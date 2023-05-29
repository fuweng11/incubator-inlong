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

package org.apache.inlong.manager.service.node.tencent.ck;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.tencent.ck.InnerClickHouseDataNodeDTO;
import org.apache.inlong.manager.pojo.node.tencent.ck.InnerClickHouseDataNodeInfo;
import org.apache.inlong.manager.pojo.node.tencent.ck.InnerClickHouseDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InnerClickHouseOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerClickHouseOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.INNER_CK;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }

        InnerClickHouseDataNodeInfo innerClickHouseDataNodeInfo = new InnerClickHouseDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, innerClickHouseDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            InnerClickHouseDataNodeDTO dto = InnerClickHouseDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, innerClickHouseDataNodeInfo);
        }

        LOGGER.info("success to get data node info from entity");
        return innerClickHouseDataNodeInfo;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        InnerClickHouseDataNodeRequest innerClickHouseDataNodeRequest = (InnerClickHouseDataNodeRequest) request;
        CommonBeanUtils.copyProperties(innerClickHouseDataNodeRequest, targetEntity, true);
        try {
            InnerClickHouseDataNodeDTO dto = InnerClickHouseDataNodeDTO.getFromRequest(innerClickHouseDataNodeRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.info("success to set entity for hive data node");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }
    }
}
