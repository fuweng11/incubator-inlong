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

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.flink.FlinkOperation;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.enums.Constants;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.SinkExtInfo;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.plugin.util.FlinkUtils.getExceptionStackMsg;

/**
 * Listener of startup sort.
 */
@Slf4j
public class StartupSortListener implements SortOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext workflowContext) {
        ProcessForm processForm = workflowContext.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof GroupResourceProcessForm)) {
            log.info("not add startup group listener, not GroupResourceProcessForm for groupId [{}]", groupId);
            return false;
        }
        GroupResourceProcessForm groupProcessForm = (GroupResourceProcessForm) processForm;
        if (groupProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add startup group listener, as the operate was not INIT for groupId [{}]", groupId);
            return false;
        }

        log.info("add startup group listener for groupId [{}]", groupId);
        return true;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        ProcessForm processForm = context.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof GroupResourceProcessForm)) {
            String message = String.format("process form was not GroupResource for groupId [%s]", groupId);
            log.error(message);
            return ListenerResult.fail(message);
        }

        GroupResourceProcessForm groupResourceForm = (GroupResourceProcessForm) processForm;
        List<InlongStreamInfo> streamInfos = groupResourceForm.getStreamInfos();
        int sinkCount = streamInfos.stream()
                .map(s -> s.getSinkList() == null ? 0 : s.getSinkList().size())
                .reduce(0, Integer::sum);
        if (sinkCount == 0) {
            log.warn("not any sink configured for group {}, skip launching sort job", groupId);
            return ListenerResult.success();
        }

        for (InlongStreamInfo streamInfo : streamInfos) {
            List<StreamSink> sinkList = streamInfo.getSinkList();
            if (CollectionUtils.isEmpty(sinkList)) {
                continue;
            }
            for (StreamSink sink : streamInfo.getSinkList()) {
                if (!SinkType.SORT_FLINK_SINK.contains(sink.getSinkType())){
                    continue;
                }
                List<SinkExtInfo> sinkExtList = sink.getExtList();
                log.info("stream sink ext info: {}", sinkExtList);
                Map<String, String> kvConf = sinkExtList.stream().filter(v -> StringUtils.isNotEmpty(v.getKeyName())
                        && StringUtils.isNotEmpty(v.getKeyValue())).collect(Collectors.toMap(
                        SinkExtInfo::getKeyName,
                        SinkExtInfo::getKeyValue));

                String sortExt = kvConf.get(InlongConstants.SORT_PROPERTIES);
                if (StringUtils.isNotEmpty(sortExt)) {
                    Map<String, String> result = JsonUtils.OBJECT_MAPPER.convertValue(
                            JsonUtils.OBJECT_MAPPER.readTree(sortExt), new TypeReference<Map<String, String>>() {
                            });
                    kvConf.putAll(result);
                }

                String dataflow = kvConf.get(InlongConstants.DATAFLOW);
                if (StringUtils.isEmpty(dataflow)) {
                    String message = String.format("dataflow is empty for sinkId [%s]", sink.getId());
                    log.error(message);
                    return ListenerResult.fail(message);
                }

                FlinkInfo flinkInfo = new FlinkInfo();

                String jobName = Constants.SORT_JOB_NAME_GENERATOR.apply(processForm) + sink.getId();
                flinkInfo.setJobName(jobName);
                String sortUrl = kvConf.get(InlongConstants.SORT_URL);
                flinkInfo.setEndpoint(sortUrl);
                flinkInfo.setInlongStreamInfoList(Collections.singletonList(streamInfo));

                FlinkService flinkService = new FlinkService(flinkInfo.getEndpoint());
                FlinkOperation flinkOperation = new FlinkOperation(flinkService);

                try {
                    flinkOperation.genPath(flinkInfo, dataflow);
                    flinkOperation.start(flinkInfo);
                    log.info("job submit success for sinkId = {}, jobId = {}", sink.getId(), flinkInfo.getJobId());
                } catch (Exception e) {
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(getExceptionStackMsg(e));
                    flinkOperation.pollJobStatus(flinkInfo, JobStatus.RUNNING);

                    String message = String.format("startup sort failed for sinkId [%s]", sink.getId());
                    log.error(message, e);
                    return ListenerResult.fail(message + e.getMessage());
                }

                saveInfo(sink, InlongConstants.SORT_JOB_ID, flinkInfo.getJobId(), sinkExtList);
                flinkOperation.pollJobStatus(flinkInfo, JobStatus.RUNNING);
            }
        }
        return ListenerResult.success();
    }

    /**
     * Save sink ext info into list.
     */
    private void saveInfo(StreamSink streamSink, String keyName, String keyValue, List<SinkExtInfo> extInfoList) {
        SinkExtInfo extInfo = new SinkExtInfo();
        extInfo.setInlongGroupId(streamSink.getInlongGroupId());
        extInfo.setInlongStreamId(streamSink.getInlongStreamId());
        extInfo.setSinkId(streamSink.getId());
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }

}
