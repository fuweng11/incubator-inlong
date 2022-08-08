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

import lombok.Builder;
import lombok.Data;

/**
 * request to modify US task
 */
@Data
@Builder
public class UpdateUsTaskRequest {

    private String taskId;

    // not required, minute information in task start time (0-59)
    private int startMin;

    // task retry times and retry interval are the global configuration of us single task, and the default is 5min
    private int tryLimit;

    private String modifier;
    private String inCharge;
    private int bgId;
    private String productId;
    private String tdwAppGroup;
    private String taskExt;

    @Data
    @Builder
    public static class ExtBean {

        private String propName;
        private String propValue;
    }

}
