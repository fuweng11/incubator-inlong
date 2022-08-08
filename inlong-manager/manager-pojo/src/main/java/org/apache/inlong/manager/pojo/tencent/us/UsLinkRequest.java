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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

/**
 * Add or modify task dependent requests
 */
@Builder
public class UsLinkRequest {

    private String parentTaskId;

    private String sonTaskId;

    // normal write -1
    private int offset;

    // fixed to "Y"
    private String status;

    // dependency type 1: depends on the full cycle instance of the parent task
    // 2: depends on the last cycle instance of the parent task
    // 3: depends on any cycle instance of the parent task
    @JsonProperty("dependence_type")
    private int dependenceType;

    // operator
    @JsonProperty("in_charge")
    private String inCharge;

}
