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

package org.apache.inlong.manager.service.sink.tencent.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.tencent.InnerBaseHiveSink;
import org.apache.inlong.manager.pojo.sink.tencent.InnerBaseHiveSinkRequest;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerBaseHiveSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveSink;
import org.apache.inlong.manager.pojo.sink.tencent.thive.InnerThiveSink;
import org.apache.inlong.manager.service.resource.sort.tencent.hive.SortHiveConfigService;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.apache.inlong.manager.service.sink.tencent.us.UsTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Objects;

@Service
public class InnerBaseHiveSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerBaseHiveSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;

    @Autowired
    private UsTaskService usTaskService;

    @Autowired
    private SortHiveConfigService sortHiveConfigService;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.INNER_HIVE.equals(sinkType) || SinkType.INNER_THIVE.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.INNER_HIVE;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        String sinkType = request.getSinkType();
        Preconditions.checkTrue(this.getSinkType().equals(sinkType) || SinkType.INNER_THIVE.equals(sinkType),
                ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        InnerBaseHiveSinkRequest sinkRequest = (InnerBaseHiveSinkRequest) request;
        if (Objects.equals(SinkType.INNER_THIVE, request.getSinkType())) {
            this.setThiveParam(sinkRequest);
        }
        try {
            InnerBaseHiveSinkDTO dto = InnerBaseHiveSinkDTO.getFromRequest(sinkRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            LOGGER.error("parsing json string to sink info failed", e);
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SINK_INFO_NOT_FOUND.getMessage());
        InnerBaseHiveSink sink;
        switch (entity.getSinkType()) {
            case SinkType.INNER_HIVE:
                sink = new InnerHiveSink();
                break;
            case SinkType.INNER_THIVE:
                sink = new InnerThiveSink();
                break;
            default:
                throw new BusinessException("unsupported sink type " + entity.getSinkType());
        }
        InnerBaseHiveSinkDTO dto = InnerBaseHiveSinkDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    private void setThiveParam(InnerBaseHiveSinkRequest sinkRequest) {
        sinkRequest.setPrimaryPartition("tdbank_imp_date");
        String groupId = sinkRequest.getInlongGroupId();
        String streamId = sinkRequest.getInlongStreamId();
        InlongStreamEntity inlongStream = streamEntityMapper.selectByIdentifier(groupId, streamId);
        if (sinkRequest.getDataSeparator() == null) {
            sinkRequest.setDataSeparator(inlongStream.getDataSeparator());
        }
    }

    @Override
    public void deleteOpt(StreamSinkEntity entity, String operator) {
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(InlongConstants.DELETED_STATUS);
        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        int rowCount = sinkMapper.updateByPrimaryKeySelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("sink has already updated with groupId={}, streamId={}, name={}, curVersion={}",
                    entity.getInlongGroupId(), entity.getInlongStreamId(), entity.getSinkName(), entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        // If the type is inner hive, the US task needs to be frozen
        if (Objects.equals(entity.getSinkType(), SinkType.INNER_THIVE)) {
            freezeUsTaskForThive(entity, entity.getId(), operator);
        }
        try {
            sortHiveConfigService.deleteSortConfig(entity);
        } catch (Exception e) {
            String errMsg = String.format("delete zk config faild for sink id=%s, sink name=%s", entity.getId(),
                    entity.getSinkName());
            LOGGER.error(errMsg);
        }
        sinkFieldMapper.logicDeleteAll(entity.getId());
    }

    private void freezeUsTaskForThive(StreamSinkEntity entity, Integer id, String operator) {
        Preconditions.checkTrue(entity != null && Objects.equals(entity.getSinkType(), SinkType.INNER_THIVE),
                "not found thive storage with id " + id);
        InnerBaseHiveSinkDTO dto = InnerBaseHiveSinkDTO.getFromJson(entity.getExtParams());
        String verifiedTaskId = dto.getVerifiedTaskId();
        if (StringUtils.isNotBlank(verifiedTaskId)) {
            usTaskService.freezeUsTask(verifiedTaskId, operator);
        }
    }

}
