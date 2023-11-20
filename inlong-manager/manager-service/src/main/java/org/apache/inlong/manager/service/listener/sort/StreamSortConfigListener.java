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

package org.apache.inlong.manager.service.listener.sort;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.resource.sort.SortConfigOperator;
import org.apache.inlong.manager.service.resource.sort.SortConfigOperatorFactory;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Event listener of build the Sort config for one inlong stream,
 * such as update the form config, or build and push config to ZK, etc.
 */
@Component
public class StreamSortConfigListener implements SortOperateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSortConfigListener.class);

    @Autowired
    private SortConfigOperatorFactory operatorFactory;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        String className = processForm.getClass().getSimpleName();
        String groupId = processForm.getInlongGroupId();
        if (processForm instanceof StreamResourceProcessForm) {
            LOGGER.info("accept sort config listener as the process is {} for groupId [{}]", className, groupId);
            return true;
        } else {
            LOGGER.info("not accept sort config listener as the process is {} for groupId [{}]", className, groupId);
            return false;
        }
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        // Read the current information
        InlongGroupInfo groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            String msg = "inlong group not found with groupId=" + groupId;
            LOGGER.error(msg);
            throw new WorkflowListenerException(msg);
        }
        form.setGroupInfo(groupInfo);
        form.setStreamInfo(streamService.get(groupId, streamId));
        groupInfo = form.getGroupInfo();
        streamInfo = form.getStreamInfo();
        LOGGER.info("begin to build sort config for groupId={}, streamId={}", groupId, streamId);

        GroupOperateType operateType = form.getGroupOperateType();
        if (operateType == GroupOperateType.SUSPEND || operateType == GroupOperateType.DELETE) {
            LOGGER.info("not build sort config for groupId={}, streamId={}, as the group operate type={}",
                    groupId, streamId, operateType);
            return ListenerResult.success();
        }

        GroupStatus groupStatus = GroupStatus.forCode(groupInfo.getStatus());
        Preconditions.expectTrue(GroupStatus.CONFIG_FAILED != groupStatus,
                String.format("group status=%s not support start stream for groupId=%s", groupStatus, groupId));
        List<StreamSink> streamSinks = streamInfo.getSinkList();
        if (CollectionUtils.isEmpty(streamSinks)) {
            LOGGER.warn("not build sort config for groupId={}, streamId={}, as not found any sinks", groupId, streamId);
            return ListenerResult.success();
        }

        try {
            LOGGER.info("testsedadsad");
            List<String> sinkTypeList = streamSinks.stream().map(StreamSink::getSinkType).collect(Collectors.toList());
            List<SortConfigOperator> operatorList = operatorFactory.getInstance(sinkTypeList);
            LOGGER.info("ntest size ={} ", operatorList.size());
            for (SortConfigOperator operator : operatorList) {
                LOGGER.info("ntest s123123ize ={} ", operatorList.size());
                operator.buildConfig(groupInfo, streamInfo,
                        InlongConstants.SYNC_SEND.equals(groupInfo.getInlongGroupMode()));
            }
            for (InlongStreamExtInfo streamExtInfo : streamInfo.getExtList()) {
                LOGGER.info("ntest ext ={} ", streamExtInfo);
            }

        } catch (Exception e) {
            String msg = String.format("failed to build sort config for groupId=%s, streamId=%s, ", groupId, streamId);
            LOGGER.error(msg + "streamInfo=" + streamInfo, e);
            throw new WorkflowListenerException(msg + e.getMessage());
        }

        LOGGER.info("success to build sort config for groupId={}, streamId={}", groupId, streamId);
        return ListenerResult.success();
    }

}
