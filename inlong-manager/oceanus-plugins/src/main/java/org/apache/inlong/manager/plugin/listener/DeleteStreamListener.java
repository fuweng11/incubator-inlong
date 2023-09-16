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

import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import lombok.extern.slf4j.Slf4j;

/**
 * Listener of delete stream sort
 */
@Slf4j
public class DeleteStreamListener implements SortOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            log.info("not add delete stream listener, not StreamResourceProcessForm for groupId={}", groupId);
            return false;
        }

        StreamResourceProcessForm streamProcessForm = (StreamResourceProcessForm) processForm;
        String streamId = streamProcessForm.getStreamInfo().getInlongStreamId();
        if (streamProcessForm.getGroupOperateType() != GroupOperateType.DELETE) {
            log.info("not add delete stream listener, as the operate was not DELETE for groupId={} streamId={}",
                    groupId, streamId);
            return false;
        }

        log.info("add delete stream listener for groupId={} streamId={}", groupId, streamId);
        return true;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        return ListenerResult.success();
    }

}
