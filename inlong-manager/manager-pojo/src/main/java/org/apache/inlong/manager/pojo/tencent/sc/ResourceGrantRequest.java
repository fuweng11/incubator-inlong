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

package org.apache.inlong.manager.pojo.tencent.sc;

import com.google.common.collect.Lists;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * resource authorization request
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("resource authorization request")
public class ResourceGrantRequest {

    /**
     * library table resources of personal hive
     */
    public static final String TYPE_HIVE_USER = "grant_hive_access_user";
    /**
     * library table authorization of application group hive
     */
    public static final String TYPE_HIVE_GROUP = "grant_hive_access_group";

    @ApiModelProperty("type of authorization")
    private String type;

    @ApiModelProperty("authorized resources")
    private ScResource resource;

    @ApiModelProperty("user list")
    private List<String> users = Lists.newArrayList();

    @ApiModelProperty("app group list")
    private List<String> groups = Lists.newArrayList();

    @ApiModelProperty("permission type")
    private List<String> accesses;

    @ApiModelProperty("data access type")
    private DataAccessType dataAccessType;

    @ApiModelProperty("duration month")
    private Integer liveMonth;

    @ApiModelProperty("duration")
    private Integer duration;

    @ApiModelProperty("duration unit")
    private ChronoUnit durationUnit;

    @ApiModelProperty("authorization description")
    private String description;

    public enum DataAccessType {

        /**
         * plaintext data
         */
        PLAIN_TEXT,

        /**
         * desensitization data
         */
        DESENSITIZATION
    }
}
