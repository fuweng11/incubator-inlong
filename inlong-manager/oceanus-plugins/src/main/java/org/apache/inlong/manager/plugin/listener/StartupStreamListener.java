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

package org.apache.inlong.manager.plugin.listener;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.oceanus.OceanusOperation;
import org.apache.inlong.manager.plugin.oceanus.OceanusService;
import org.apache.inlong.manager.plugin.oceanus.dto.JobBaseInfo;
import org.apache.inlong.manager.plugin.util.OceanusUtils;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Listener for startup the Sort task for InlongStream
 */
@Slf4j
public class StartupStreamListener implements SortOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    /**
     * Currently, the process of starting Sort tasks has been initiated in {@link StartupSortListener}.
     * <p/>Because the Sort task is only associated with InlongGroup, no need to start it for InlongStream.
     */
    @Override
    public boolean accept(WorkflowContext workflowContext) {
        ProcessForm processForm = workflowContext.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            log.info("not add startup stream listener, not StreamResourceProcessForm for groupId [{}]", groupId);
            return false;
        }

        StreamResourceProcessForm streamProcessForm = (StreamResourceProcessForm) processForm;
        String streamId = streamProcessForm.getStreamInfo().getInlongStreamId();
        if (streamProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add startup stream listener, as the operate was not INIT for groupId [{}] streamId [{}]",
                    groupId, streamId);
            return false;
        }

        log.info("add startup stream listener for groupId [{}] streamId [{}]", groupId, streamId);
        return InlongConstants.STANDARD_MODE.equals(streamProcessForm.getGroupInfo().getInlongGroupMode());
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        InlongGroupInfo groupInfo = form.getGroupInfo();
        int sinkCount = streamInfo.getSinkList() == null ? 0 : streamInfo.getSinkList().size();
        if (sinkCount == 0) {
            log.warn("not any sink configured for group {}, skip launching sort job", groupInfo.getInlongGroupId());
            return ListenerResult.success();
        }

        List<StreamSink> sinkList = streamInfo.getSinkList();
        List<String> sinkTypes = sinkList.stream().map(StreamSink::getSinkType).collect(Collectors.toList());
        boolean isSortFlink = false;
        for (String sinkType : sinkTypes) {
            if (SinkType.SORT_FLINK_SINK.contains(sinkType)) {
                isSortFlink = true;
            }
        }
        if (CollectionUtils.isEmpty(sinkList) || !isSortFlink) {
            return ListenerResult.success();
        }
        if (!InlongConstants.STANDARD_MODE.equals(groupInfo.getInlongGroupMode())) {
            log.warn("not need start up sink for groupId ={}, streamId ={}, when mode = {}, skip launching sort job",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId(), groupInfo.getInlongGroupMode());
            return ListenerResult.success();
        }

        List<InlongStreamExtInfo> extList = streamInfo.getExtList();
        log.info("stream sink ext info: {}", extList);

        Map<String, String> kvConf = extList.stream().filter(v -> StringUtils.isNotEmpty(v.getKeyName())
                && StringUtils.isNotEmpty(v.getKeyValue())).collect(Collectors.toMap(
                        InlongStreamExtInfo::getKeyName,
                        InlongStreamExtInfo::getKeyValue));
        String sortExt = kvConf.get(InlongConstants.SORT_PROPERTIES);
        if (StringUtils.isNotEmpty(sortExt)) {
            Map<String, String> result = JsonUtils.OBJECT_MAPPER.convertValue(
                    JsonUtils.OBJECT_MAPPER.readTree(sortExt), new TypeReference<Map<String, String>>() {
                    });
            kvConf.putAll(result);
        }

        String dataflow = kvConf.get(InlongConstants.DATAFLOW);
        if (StringUtils.isEmpty(dataflow)) {
            String message = String.format("dataflow is empty for groupId [%s], streamId [%s]",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
            log.error(message);
            return ListenerResult.fail(message);
        }

        // todo
        String jobName = groupInfo.getInlongGroupId() + "_" + streamInfo.getInlongStreamId();
        JobBaseInfo jobBaseInfo = OceanusUtils.initParamForOceanus(kvConf);
        if (InlongConstants.STANDARD_MODE.equals(groupInfo.getInlongGroupMode())) {
            jobBaseInfo = OceanusUtils.initParamsForStandardMode(jobBaseInfo, kvConf);
        }
        jobBaseInfo.setName(jobName);
        jobBaseInfo.setOperator(groupInfo.getCreator());
        OceanusService oceanusService = new OceanusService();
        jobBaseInfo.setFileId(oceanusService.uploadFile(jobBaseInfo, dataflow));
        saveInfo(streamInfo, "sort.job.fileId", jobBaseInfo.getFileId().toString(), extList);

        OceanusOperation oceanusOperation = new OceanusOperation(oceanusService);

        try {
            oceanusOperation.start(jobBaseInfo,
                    InlongConstants.DATASYNC_MODE.equals(groupInfo.getInlongGroupMode()));
            log.info("job submit success, jobId is [{}]", jobBaseInfo.getJobId());
        } catch (Exception e) {
            String message = String.format("startup sort failed for groupId [%s] ", groupInfo.getInlongGroupId());
            log.error(message, e);
            return ListenerResult.fail(message + e.getMessage());
        }
        saveInfo(streamInfo, InlongConstants.SORT_JOB_ID, jobBaseInfo.getJobId().toString(), extList);
        return ListenerResult.success();
    }

    /**
     * Save stream ext info into list.
     */
    private void saveInfo(InlongStreamInfo streamInfo, String keyName, String keyValue,
            List<InlongStreamExtInfo> extInfoList) {
        InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
        extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
        extInfo.setInlongStreamId(streamInfo.getInlongStreamId());
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }

}
