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

package org.apache.inlong.manager.service.node.tencent.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeRequest;
import org.apache.inlong.manager.pojo.node.tencent.hive.InnerBaseHiveDataNodeDTO;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InnerBaseHiveDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerBaseHiveDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType) || DataNodeType.INNER_THIVE.equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.INNER_HIVE;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }

        InnerBaseHiveDataNodeInfo innerBaseHiveDataNodeInfo = new InnerBaseHiveDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, innerBaseHiveDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            InnerBaseHiveDataNodeDTO dto = InnerBaseHiveDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, innerBaseHiveDataNodeInfo);
        }

        LOGGER.info("success to get data node info from entity");
        return innerBaseHiveDataNodeInfo;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        InnerBaseHiveDataNodeRequest innerBaseHiveDataNodeRequest = (InnerBaseHiveDataNodeRequest) request;
        CommonBeanUtils.copyProperties(innerBaseHiveDataNodeRequest, targetEntity, true);
        try {
            InnerBaseHiveDataNodeDTO dto = InnerBaseHiveDataNodeDTO.getFromRequest(innerBaseHiveDataNodeRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
            LOGGER.info("success to set entity for hive data node");
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }
    }
}
