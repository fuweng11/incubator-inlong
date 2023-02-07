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

package org.apache.inlong.manager.service.sink.tencent.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.custom.CustomSink;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Custom sink operator.
 */
@Service
public class CustomSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        try {
            targetEntity.setExtParams(objectMapper.writeValueAsString(request.getProperties()));
        } catch (Exception e) {
            LOGGER.error("parsing json string to sink info failed", e);
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
        }
    }

    @Override
    protected String getSinkType() {
        return SinkType.CUSTOM;
    }

    @Override
    public Boolean accept(String sinkType) {
        return sinkType.startsWith(SinkType.CUSTOM);
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        CustomSink sink = new CustomSink();
        if (entity == null) {
            return sink;
        }
        CommonBeanUtils.copyProperties(entity, sink, true);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            sink.setProperties(JsonUtils.parseObject(entity.getExtParams(), Map.class));
        }
        return sink;
    }

    @Override
    public Integer saveOpt(SinkRequest request, String operator) {
        StreamSinkEntity entity = CommonBeanUtils.copyProperties(request, StreamSinkEntity::new);
        entity.setStatus(SinkStatus.NEW.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setSinkType(request.getSinkType());
        // get the ext params
        setTargetEntity(request, entity);
        sinkMapper.insert(entity);
        Integer sinkId = entity.getId();
        request.setId(sinkId);
        return sinkId;
    }

    @Override
    public List<SinkField> getSinkFields(Integer sinkId) {
        return null;
    }

    @Override
    public void updateFieldOpt(Boolean onlyAdd, SinkRequest request) {
        // no opt
    }

    @Override
    public void deleteOpt(StreamSinkEntity entity, String operator) {
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(InlongConstants.DELETED_STATUS);
        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        int rowCount = sinkMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("sink has already updated with groupId={}, streamId={}, name={}, curVersion={}",
                    entity.getInlongGroupId(), entity.getInlongStreamId(), entity.getSinkName(), entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        sinkFieldMapper.logicDeleteAll(entity.getId());
    }

    @Override
    public void saveFieldOpt(SinkRequest request) {
        // no opt
    }
}
