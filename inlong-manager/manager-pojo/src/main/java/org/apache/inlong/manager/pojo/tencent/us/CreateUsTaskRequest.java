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

package org.apache.inlong.manager.pojo.tencent.us;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Request to create us
 */
@Data
@Builder
public class CreateUsTaskRequest {

    private String taskType;
    private String taskName;
    private String taskAction;
    private String cycleNum;
    private String cycleUnit;
    private String startDate;

    // not required.
    // The start time of monthly and weekly tasks.
    // The monthly task is a day in the month, and the weekly task is a day in the week
    private int startDay;

    // not required, hour information in task start time (0-23)
    private int startHour;

    // not required, minute information in task start time (0-59)
    private int startMin;

    // parent task ID, JSON format, key is the parent task ID,
    // value is the dependent type, such as: {'20200803173718028': 1}
    // 1: Rely on the full cycle instance of the parent task (most commonly used)
    // 2: rely on the last cycle instance of the parent task 3: rely on any cycle instance of the parent task
    private String parentTaskId;

    // self dependence: 1-self dependence, 2-single instance operation, 3-multi instance operation
    private int selfDepend;

    // task retry times and retry interval are the global configuration of us single task, and the default is 5min
    private int tryLimit;

    private String creater;
    private String inCharge;
    private int bgId;
    private String productId;
    private String tdwAppGroup;
    private String taskExt;

    @Data
    @Builder
    @AllArgsConstructor
    public static class TaskExt {

        private String propName;
        private String propValue;
    }

}
