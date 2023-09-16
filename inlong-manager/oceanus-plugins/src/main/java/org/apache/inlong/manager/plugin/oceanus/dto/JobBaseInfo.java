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

package org.apache.inlong.manager.plugin.oceanus.dto;

import lombok.Data;

@Data
public class JobBaseInfo {

    private Long id;
    private Long projectId;
    private Long jobId;
    private Long fileId;
    private String name;
    private String jobType = "JAR";
    private String description;
    private String flinkVersion = "1.13";
    private String executionMode = "STREAMING";

    private String stateType = "ROCKSDB";
    private String checkpointMode = "EXACTLY_ONCE";
    private Integer checkpointTimeout = 60000;
    private Boolean enableCheckpointing = false;
    private Integer checkpointInterval = 60000;

    private String sortDistJarId;
    private String sortSourceConnectId;
    private String sortSinkConnectId;

    private String operator;

}
