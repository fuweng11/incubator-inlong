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

package org.apache.inlong.manager.service.sink.tencent.us;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSinkDTO;
import org.apache.inlong.manager.pojo.tencent.sc.AppGroup;
import org.apache.inlong.manager.pojo.tencent.us.CreateUsTaskRequest;
import org.apache.inlong.manager.pojo.tencent.us.CreateUsTaskRequest.TaskExt;
import org.apache.inlong.manager.pojo.tencent.us.UpdateUsTaskRequest;
import org.apache.inlong.manager.pojo.tencent.us.UsTaskInfo;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.resource.sc.ScService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class UsTaskOperationService {

    @Autowired
    private InlongGroupService inlongGroupService;
    @Autowired
    private UsTaskService usTaskService;
    @Autowired
    private ScService scService;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private ObjectMapper objectMapper;

    // iceberg check task type
    private static final String ICEBERG_CHECK_TYPE = "209";

    public String createUsTaskBySink(Integer sinkId) {
        StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);
        if (sinkEntity == null) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_NOT_FOUND);
        }
        String groupId = sinkEntity.getInlongGroupId();

        InlongGroupInfo groupInfo = inlongGroupService.get(sinkEntity.getInlongGroupId());
        if (groupInfo == null) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        UsTaskInfo usTaskInfo = new UsTaskInfo();
        switch (sinkEntity.getSinkType()) {
            case SinkType.INNER_ICEBERG:
                InnerIcebergSinkDTO innerIcebergSinkDTO = InnerIcebergSinkDTO.getFromJson(sinkEntity.getExtParams());
                CommonBeanUtils.copyProperties(innerIcebergSinkDTO, usTaskInfo, true);
                usTaskInfo.setTaskId(innerIcebergSinkDTO.getIcebergCheckerTaskId());
                usTaskInfo.setAppGroupName(innerIcebergSinkDTO.getResourceGroup());
                break;
            case SinkType.ICEBERG:
                IcebergSinkDTO icebergSinkDTO = IcebergSinkDTO.getFromJson(sinkEntity.getExtParams());
                CommonBeanUtils.copyProperties(icebergSinkDTO, usTaskInfo, true);
                usTaskInfo.setTaskId(icebergSinkDTO.getIcebergCheckerTaskId());
                usTaskInfo.setAppGroupName(icebergSinkDTO.getResourceGroup());
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT);
        }
        log.info("test bgid ={}", usTaskInfo.getBgId());
        if (StringUtils.isBlank(usTaskInfo.getAppGroupName())) {
            Integer clusterId = scService.getClusterIdByIdentifier(usTaskInfo.getClusterTag());
            AppGroup appGroup = scService.getAppGroup(clusterId, groupInfo.getAppGroupName());
            groupInfo.setBgId(appGroup.getBgId());
        } else {
            groupInfo.setAppGroupName(usTaskInfo.getAppGroupName());
            groupInfo.setBgId(usTaskInfo.getBgId());
        }
        // extended parameters of task
        List<TaskExt> extList = new ArrayList<>();
        extList.add(new TaskExt("spark_version", "spark3.3-gaia2.8"));
        extList.add(new TaskExt("datePattern", usTaskInfo.getDatePattern()));
        extList.add(new TaskExt("interval", usTaskInfo.getCycleNum()));
        extList.add(new TaskExt("tableName", usTaskInfo.getTableName()));
        extList.add(new TaskExt("gaia_id", usTaskInfo.getGaiaId()));
        String inCharges;
        if (StringUtils.isNotEmpty(groupInfo.getInCharges())) {
            inCharges = groupInfo.getInCharges().replace(",", ";");
        } else {
            inCharges = groupInfo.getCreator();
        }
        if (StringUtils.isBlank(usTaskInfo.getTaskId())) {
            log.info("test groupinfo ={}", usTaskInfo);
            CreateUsTaskRequest createRequest = usTaskService.getCreateTaskRequestForIceberg(groupInfo,
                    usTaskInfo,
                    inCharges,
                    extList);
            createRequest.setTaskType(ICEBERG_CHECK_TYPE);
            createRequest.setTaskAction("Inlong iceberg check task");
            createRequest.setTaskName(
                    sinkEntity.getInlongGroupId() + "-" + sinkEntity.getInlongStreamId() + "-iceberg-check-"
                            + System.currentTimeMillis());
            // empty JSON string, otherwise US parsing error
            createRequest.setParentTaskId("{}");
            createRequest.setSelfDepend(2);
            usTaskInfo.setTaskId(usTaskService.createUsTask(createRequest));

            // write task ID to database
            if (StringUtils.isNotBlank(usTaskInfo.getTaskId())) {
                try {
                    switch (sinkEntity.getSinkType()) {
                        case SinkType.INNER_ICEBERG:
                            InnerIcebergSinkDTO innerIcebergSinkDTO = InnerIcebergSinkDTO.getFromJson(
                                    sinkEntity.getExtParams());
                            innerIcebergSinkDTO.setIcebergCheckerTaskId(usTaskInfo.getTaskId());
                            sinkEntity.setExtParams(objectMapper.writeValueAsString(innerIcebergSinkDTO));
                            break;
                        case SinkType.ICEBERG:
                            IcebergSinkDTO icebergSinkDTO = IcebergSinkDTO.getFromJson(sinkEntity.getExtParams());
                            icebergSinkDTO.setIcebergCheckerTaskId(usTaskInfo.getTaskId());
                            sinkEntity.setExtParams(objectMapper.writeValueAsString(icebergSinkDTO));
                            break;
                        default:
                            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT);
                    }
                    sinkEntityMapper.updateByIdSelective(sinkEntity);
                } catch (Exception e) {
                    log.error("parsing json string to sink info failed", e);
                    throw new WorkflowListenerException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
                }
            }
        } else {
            // If the task ID exists, update the existing task
            // the modifier must be in the [US person in charge before modification],
            // otherwise there is no permission to operate - if the task is frozen, unfreeze it first
            usTaskService.unfreezeUsTask(usTaskInfo.getTaskId(), inCharges.split(";")[0]);
            UpdateUsTaskRequest updateRequest =
                    usTaskService.getUpdateTaskRequestForIceberg(usTaskInfo.getTaskId(), groupInfo, inCharges,
                            extList);
            usTaskService.updateUsTask(updateRequest);
        }
        return usTaskInfo.getTaskId();
    }

    public void freezeUsTaskBySink(Integer sinkId) {
        StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);
        if (sinkEntity == null) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_NOT_FOUND);
        }
        String groupId = sinkEntity.getInlongGroupId();

        InlongGroupInfo groupInfo = inlongGroupService.get(groupId);
        if (groupInfo == null) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        String taskId = "";
        switch (sinkEntity.getSinkType()) {
            case SinkType.INNER_ICEBERG:
                InnerIcebergSinkDTO innerIcebergSinkDTO = InnerIcebergSinkDTO.getFromJson(sinkEntity.getExtParams());
                taskId = innerIcebergSinkDTO.getIcebergCheckerTaskId();
                break;
            case SinkType.ICEBERG:
                IcebergSinkDTO icebergSinkDTO = IcebergSinkDTO.getFromJson(sinkEntity.getExtParams());
                taskId = icebergSinkDTO.getIcebergCheckerTaskId();
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT);
        }
        if (StringUtils.isNotBlank(taskId)) {
            usTaskService.freezeUsTask(taskId, groupInfo.getCreator());
        }
    }

}
