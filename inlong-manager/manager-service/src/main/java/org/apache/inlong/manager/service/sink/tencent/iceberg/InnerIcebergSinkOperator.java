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

package org.apache.inlong.manager.service.sink.tencent.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSink;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSinkRequest;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InnerIcebergSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerIcebergSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private SortHiveConfigService sortIcebergConfigService;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.INNER_ICEBERG.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.INNER_ICEBERG;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        Preconditions.expectTrue(this.getSinkType().equals(request.getSinkType()),
                ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        InnerIcebergSinkRequest sinkRequest = (InnerIcebergSinkRequest) request;
        try {
            InnerIcebergSinkDTO dto = InnerIcebergSinkDTO.getFromRequest(sinkRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            LOGGER.error("parsing json string to sink info failed", e);
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        InnerIcebergSink sink = new InnerIcebergSink();
        if (entity == null) {
            return sink;
        }

        InnerIcebergSinkDTO dto = InnerIcebergSinkDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public void deleteOpt(StreamSinkEntity entity, String operator) {
        try {
            sortIcebergConfigService.deleteSortConfig(entity);
        } catch (Exception e) {
            String errMsg = String.format("delete zk config failed for sink id=%s, sink name=%s", entity.getId(),
                    entity.getSinkName());
            LOGGER.error(errMsg, e);
        }
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
}
