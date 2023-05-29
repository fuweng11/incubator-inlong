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

package org.apache.inlong.manager.plugin.common.pojo.user;

import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

import java.util.Date;
import java.util.Set;

/**
 * Login user info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StaffDTO {

    @ApiModelProperty(value = "user name")
    private String username;

    @ApiModelProperty(value = "BG id")
    private String bgId;

    @ApiModelProperty(value = "BG name")
    private String bgName;

    @ApiModelProperty(value = "BG English name")
    private String bgEnName;

    @ApiModelProperty(value = "department id")
    private Integer deptId;

    @ApiModelProperty(value = "department name")
    private String deptName;

    @ApiModelProperty(value = "department full id")
    private String deptFullId;

    @ApiModelProperty(value = "department full name")
    private String deptFullName;

    @ApiModelProperty(value = "last login time")
    private Date lastLoginTime;

    @ApiModelProperty(value = "user role")
    private Set<String> roles;

    @ApiModelProperty(value = "proxy user")
    private String proxyUser;

    @ApiModelProperty(value = "authentication type")
    private AuthenticationType authenticationType;

    /**
     * Get the dto instance from the json
     */
    public static StaffDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, StaffDTO.class);
    }

    /**
     * Convert the dto instance to string
     */
    public static String convertToJson(StaffDTO dto) {
        return JsonUtils.toJsonString(dto);
    }
}
