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

package org.apache.inlong.manager.service.sink.iceberg;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.pojo.node.iceberg.IcebergDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergColumnInfo;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSink;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSinkDTO;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSinkRequest;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Iceberg sink operator, such as save or update iceberg field, etc.
 */
@Service
public class IcebergSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSinkOperator.class);

    private static final String CATALOG_TYPE_HIVE = "HIVE";

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.ICEBERG.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.ICEBERG;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        IcebergSinkRequest sinkRequest = (IcebergSinkRequest) request;
        try {
            IcebergSinkDTO dto = IcebergSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Iceberg SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        IcebergSink sink = new IcebergSink();
        if (entity == null) {
            return sink;
        }

        IcebergSinkDTO dto = IcebergSinkDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getCatalogUri()) && CATALOG_TYPE_HIVE.equals(dto.getCatalogType())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                        "iceberg catalog uri unspecified and data node is blank");
            }
            IcebergDataNodeInfo dataNodeInfo = (IcebergDataNodeInfo) dataNodeHelper.getDataNodeInfo(
                    entity.getDataNodeName(), entity.getSinkType());
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setCatalogUri(dataNodeInfo.getUrl());
        }

        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    protected void checkFieldInfo(SinkField field) {
        if (FieldType.forName(field.getFieldType()) == FieldType.DECIMAL) {
            IcebergColumnInfo info = IcebergColumnInfo.getFromJson(field.getExtParams());
            if (info.getPrecision() == null || info.getScale() == null) {
                String errorMsg = String.format("precision or scale not specified for decimal field (%s)",
                        field.getFieldName());
                LOGGER.error("field info check error: {}", errorMsg);
                throw new BusinessException(errorMsg);
            }
            if (info.getPrecision() < info.getScale()) {
                String errorMsg = String.format(
                        "precision (%d) must be greater or equal than scale (%d) for decimal field (%s)",
                        info.getPrecision(), info.getScale(), field.getFieldName());
                LOGGER.error("field info check error: {}", errorMsg);
                throw new BusinessException(errorMsg);
            }
        }
    }

    @Override
    public void saveFieldOpt(SinkRequest request) {
        List<SinkField> fieldList = request.getSinkFieldList();
        LOGGER.info("test begin to save sink fields=");
        LOGGER.info("begin to save es sink fields={}", fieldList);
        if (CollectionUtils.isEmpty(fieldList)) {
            return;
        }

        int size = fieldList.size();
        List<StreamSinkFieldEntity> entityList = new ArrayList<>(size);
        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String sinkType = request.getSinkType();
        Integer sinkId = request.getId();
        for (SinkField fieldInfo : fieldList) {
            this.checkFieldInfo(fieldInfo);
            StreamSinkFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, StreamSinkFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            try {
                IcebergColumnInfo dto = IcebergColumnInfo.getFromRequest(fieldInfo);
                LOGGER.info("tset to save es dto fields={}", dto);
                String ext = objectMapper.writeValueAsString(dto);
                LOGGER.info("tesr ext {}", ext);
                fieldEntity.setExtParams(ext);
                LOGGER.info("tset to save es dto ext={}", fieldEntity.getExtParams());
            } catch (Exception e) {
                LOGGER.error("parsing json string to sink field info failed", e);
                throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
            }
            fieldEntity.setInlongGroupId(groupId);
            fieldEntity.setInlongStreamId(streamId);
            fieldEntity.setSinkType(sinkType);
            fieldEntity.setSinkId(sinkId);
            fieldEntity.setIsDeleted(InlongConstants.UN_DELETED);
            entityList.add(fieldEntity);
        }
        LOGGER.info("tset to save es sink fields={}", entityList);

        sinkFieldMapper.insertAll(entityList);
        LOGGER.info("success to save es sink fields");
    }

    @Override
    public List<SinkField> getSinkFields(Integer sinkId) {
        List<StreamSinkFieldEntity> sinkFieldEntities = sinkFieldMapper.selectBySinkId(sinkId);
        List<SinkField> fieldList = new ArrayList<>();
        if (CollectionUtils.isEmpty(sinkFieldEntities)) {
            return fieldList;
        }
        sinkFieldEntities.forEach(field -> {
            SinkField sinkField = new SinkField();
            if (StringUtils.isNotBlank(field.getExtParams())) {
                IcebergColumnInfo icebergColumnInfo = IcebergColumnInfo.getFromJson(
                        field.getExtParams());
                CommonBeanUtils.copyProperties(field, icebergColumnInfo, true);
                fieldList.add(icebergColumnInfo);
            } else {
                CommonBeanUtils.copyProperties(field, sinkField, true);
                fieldList.add(sinkField);
            }

        });
        return fieldList;
    }
}
