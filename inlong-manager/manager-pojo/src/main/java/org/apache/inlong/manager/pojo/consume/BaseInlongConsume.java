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

package org.apache.inlong.manager.pojo.consume;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * The base parameter class of InlongConsume, support user extend their own business params.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base info of inlong consume")
public class BaseInlongConsume {

    // you can add extend parameters in this class
    @ApiModelProperty(value = "bg info")
    private String bg;

    @ApiModelProperty(value = "product id")
    private Integer productId;

    @ApiModelProperty(value = "product name")
    private String productName;

    @ApiModelProperty(value = "usage type")
    private String usageType;

    @ApiModelProperty(value = "usage type name")
    private String usageTypeName;

    @ApiModelProperty(value = "usage desc")
    private String usageDesc;

    @ApiModelProperty(value = "resource app group name")
    private String resourceAppGroupName;

    @ApiModelProperty(value = "alertEnabled")
    // @NotNull(message = "alertEnabled cannot be null")
    private Integer alertEnabled = 0;

    @ApiModelProperty(value = "alertType")
    private String alertType;

    @ApiModelProperty(value = "startTimes")
    private Integer startTimes;

    @ApiModelProperty(value = "upgradeTimes")
    private Integer upgradeTimes;

    @ApiModelProperty(value = "resetPeriod")
    private Integer resetPeriod;

    @ApiModelProperty(value = "threshold")
    private Long threshold;

    @ApiModelProperty(value = "maskSwitch")
    private Integer maskSwitch;

    @ApiModelProperty(value = "maskStartTime")
    private Date maskStartTime;

    @ApiModelProperty(value = "maskEndTime")
    private Date maskEndTime;

}
