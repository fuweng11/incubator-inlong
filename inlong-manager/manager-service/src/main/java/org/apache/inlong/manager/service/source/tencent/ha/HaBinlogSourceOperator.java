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

package org.apache.inlong.manager.service.source.tencent.ha;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterDTO;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.tencent.ha.HaBinlogSource;
import org.apache.inlong.manager.pojo.source.tencent.ha.HaBinlogSourceDTO;
import org.apache.inlong.manager.pojo.source.tencent.ha.HaBinlogSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

/**
 * HA binlog source operator
 */
@Service
public class HaBinlogSourceOperator extends AbstractSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaBinlogSourceOperator.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private InlongClusterEntityMapper inlongClusterMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.HA_BINLOG.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.HA_BINLOG;
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        HaBinlogSourceRequest sourceRequest = (HaBinlogSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
        try {
            targetEntity.setStartPosition(objectMapper.writeValueAsString(sourceRequest.getBinlogStartPosition()));
            HaBinlogSourceDTO dto = HaBinlogSourceDTO.getFromRequest(sourceRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        HaBinlogSource source = new HaBinlogSource();
        if (entity == null) {
            return source;
        }

        HaBinlogSourceDTO dto = HaBinlogSourceDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, source, true);
        CommonBeanUtils.copyProperties(dto, source, true);

        List<StreamField> sourceFields = super.getSourceFields(entity.getId());
        source.setFieldList(sourceFields);
        return source;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer saveOpt(SourceRequest request, Integer groupStatus, String operator) {
        StreamSourceEntity entity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        if (SourceType.AUTO_PUSH.equals(request.getSourceType())) {
            // auto push task needs not be issued to agent
            entity.setStatus(SourceStatus.SOURCE_NORMAL.getCode());
        } else if (GroupStatus.forCode(groupStatus).equals(GroupStatus.CONFIG_SUCCESSFUL)) {
            entity.setStatus(SourceStatus.TO_BE_ISSUED_ADD.getCode());
        } else {
            entity.setStatus(SourceStatus.SOURCE_NEW.getCode());
        }
        String clusterName = entity.getInlongClusterName();
        String dataNodeName = entity.getDataNodeName();
        List<StreamSourceEntity> existList = sourceMapper.selectByClusterAndDataNode(null,
                request.getDataNodeName(), SourceType.HA_BINLOG);
        if (CollectionUtils.isNotEmpty(existList)) {
            for (StreamSourceEntity sourceEntity : existList) {
                boolean equals = Objects.equals(clusterName, sourceEntity.getInlongClusterName());
                String msg = String.format("invalid cluster, because serverName [%s] already used by clusterName [%s]",
                        clusterName, dataNodeName);
                if (!equals) {
                    throw new BusinessException(msg);
                }
            }

        } else {
            // For the newly added serverId, modify the version number of the associated cluster to support task pull
            updateServerVersionForCluster(dataNodeName, clusterName);
        }
        entity.setCreator(operator);
        entity.setModifier(operator);

        // get the ext params
        setTargetEntity(request, entity);
        sourceMapper.insert(entity);
        saveFieldOpt(entity, request.getFieldList());
        return entity.getId();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void updateOpt(SourceRequest request, Integer groupStatus, Integer groupMode, String operator) {
        StreamSourceEntity entity = sourceMapper.selectByIdForUpdate(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());

        if (SourceType.AUTO_PUSH.equals(entity.getSourceType())) {
            LOGGER.warn("auto push source {} can not be updated", entity.getSourceName());
            return;
        }

        boolean allowUpdate = InlongConstants.LIGHTWEIGHT_MODE.equals(groupMode)
                || SourceStatus.ALLOWED_UPDATE.contains(entity.getStatus());
        if (!allowUpdate) {
            throw new BusinessException(String.format("source=%s is not allowed to update, "
                    + "please wait until its changed to final status or stop / frozen / delete it firstly", entity));
        }
        String errMsg = String.format("source has already updated with groupId=%s, streamId=%s, name=%s, curVersion=%s",
                request.getInlongGroupId(), request.getInlongStreamId(), request.getSourceName(), request.getVersion());
        if (!Objects.equals(entity.getVersion(), request.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        // source type cannot be changed
        if (!Objects.equals(entity.getSourceType(), request.getSourceType())) {
            throw new BusinessException(String.format("source type=%s cannot change to %s",
                    entity.getSourceType(), request.getSourceType()));
        }

        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String sourceName = request.getSourceName();
        List<StreamSourceEntity> sourceList = sourceMapper.selectByRelatedId(groupId, streamId, sourceName);
        for (StreamSourceEntity sourceEntity : sourceList) {
            Integer sourceId = sourceEntity.getId();
            if (!Objects.equals(sourceId, request.getId())) {
                String err = "source name=%s already exists with the groupId=%s streamId=%s";
                throw new BusinessException(String.format(err, sourceName, groupId, streamId));
            }
        }
        final String oriDataNodeName = entity.getDataNodeName();
        final String oriClusterName = entity.getInlongClusterName();
        // setting updated parameters of stream source entity.
        setTargetEntity(request, entity);
        entity.setModifier(operator);

        entity.setPreviousStatus(entity.getStatus());

        // re-issue task if necessary
        if (InlongConstants.STANDARD_MODE.equals(groupMode)) {
            if (GroupStatus.forCode(groupStatus).equals(GroupStatus.CONFIG_SUCCESSFUL)) {
                entity.setStatus(SourceStatus.TO_BE_ISSUED_RETRY.getCode());
            } else {
                switch (SourceStatus.forCode(entity.getStatus())) {
                    case SOURCE_NORMAL:
                        entity.setStatus(SourceStatus.TO_BE_ISSUED_RETRY.getCode());
                        break;
                    case SOURCE_FAILED:
                        entity.setStatus(SourceStatus.SOURCE_NEW.getCode());
                        break;
                    default:
                        // others leave it be
                        break;
                }
            }
        }

        int rowCount = sourceMapper.updateByPrimaryKeySelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.warn(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        updateFieldOpt(entity, request.getFieldList());
        // After saving, judge whether the associated cluster of [Before Modification] and [After Modification] exists,
        // and if not, update the version number
        updateServerVersionForCluster(oriDataNodeName, oriClusterName);
        updateServerVersionForCluster(entity.getDataNodeName(), entity.getInlongClusterName());
        LOGGER.info("success to update source of type={}", request.getSourceType());
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public void deleteOpt(StreamSourceEntity entity, String operator) {
        super.deleteOpt(entity, operator);
        updateServerVersionForCluster(entity.getDataNodeName(), entity.getInlongClusterName());
    }

    /**
     * If there are no data node name related tasks (add/delete) under the cluster name,
     * increase the cluster version number
     */
    private void updateServerVersionForCluster(String serverName, String clusterName) {
        List<StreamSourceEntity> existList = sourceMapper.selectByClusterAndDataNode(clusterName, serverName,
                SourceType.HA_BINLOG);
        if (CollectionUtils.isEmpty(existList)) {
            LOGGER.info("not found db source with serverName={} clusterName={}, add server version",
                    serverName, clusterName);
            InlongClusterEntity clusterEntity = inlongClusterMapper.selectByNameAndType(
                    clusterName, ClusterType.AGENT);
            try {
                AgentClusterDTO agentClusterDTO;
                if (StringUtils.isBlank(clusterEntity.getExtParams())) {
                    agentClusterDTO = new AgentClusterDTO();
                } else {
                    agentClusterDTO = AgentClusterDTO.getFromJson(clusterEntity.getExtParams());
                    agentClusterDTO.setServerVersion(agentClusterDTO.getServerVersion() + 1);
                }
                clusterEntity.setExtParams(objectMapper.writeValueAsString(agentClusterDTO));
                inlongClusterMapper.updateByIdSelective(clusterEntity);
            } catch (Exception e) {
                throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
            }
        }
    }
}
