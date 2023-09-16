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

package org.apache.inlong.manager.plugin.flink.enums;

import com.google.common.collect.Sets;
import lombok.Getter;

import java.util.Set;

@Getter
public enum OceanusJobStatus {

    SUBMITTING("SUBMITTING", "submitting"),
    LAUNCHING("LAUNCHING", "launching"),
    RUNNING("RUNNING", "running"),
    RESTARTING("RESTARTING", "restarting"),
    RECOVERING("RECOVERING", "recovering"),
    FAILING("FAILING", "failing"),
    CANCELLING("CANCELLING", "cancelling"),
    FINISHED("FINISHED", "finished"),
    FAILED("FAILED", "failed"),
    CANCELLED("CANCELLED", "cancelled");


    public static final Set<String> STARING_STATUS = Sets.newHashSet(SUBMITTING.getStatus(), LAUNCHING.getStatus());

    /**
     * The set of status from temporary to normal.
     */
    public static final Set<String> RUNNING_STATUS = Sets.newHashSet(RUNNING.getStatus(), RESTARTING.getStatus(),
            RECOVERING.getStatus(), FAILING.getStatus(), CANCELLING.getStatus());

    public static final Set<String> END_STATUS = Sets.newHashSet(FINISHED.getStatus(), FAILED.getStatus(), CANCELLED.getStatus());

    private String status;
    private String description;

    OceanusJobStatus(String status, String description) {
        this.status = status;
        this.description = description;
    }
}
