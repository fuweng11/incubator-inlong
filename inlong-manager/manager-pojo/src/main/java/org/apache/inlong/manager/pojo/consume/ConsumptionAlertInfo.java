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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Data consumption alarm configuration information")
public class ConsumptionAlertInfo {

    @ApiModelProperty(value = "consumption id")
    private Integer consumptionId;

    @ApiModelProperty(value = "consumer group")
    private String consumerGroup;

    @ApiModelProperty(value = "inlong group id")
    @NotBlank(message = "inlong group id cannot be null")
    private String inlongGroupId;

    @ApiModelProperty(value = "mq type")
    @NotBlank(message = "mq type cannot be null")
    private String mqType;

    @ApiModelProperty(value = "topic")
    private String topic;

    @ApiModelProperty(value = "Whether to configure consumption alarms")
    private Integer alertEnabled;

    @ApiModelProperty(value = "alert type")
    private String alertType;

    @ApiModelProperty(value = "start times")
    private Integer startTimes;

    @ApiModelProperty(value = "upgrade times")
    private Integer upgradeTimes;

    @ApiModelProperty(value = "reset period")
    private Integer resetPeriod;

    @ApiModelProperty(value = "threshold")
    private Long threshold;

    @ApiModelProperty(value = "mask switch")
    private Integer maskSwitch;

    @ApiModelProperty(value = "mask start time")
    private Date maskStartTime;

    @ApiModelProperty(value = "mask end time")
    private Date maskEndTime;

    @ApiModelProperty(value = "creator")
    private String creator;

    @ApiModelProperty(value = "modifier")
    private String modifier;

    @ApiModelProperty(value = "createTime")
    private Date createTime;

    @ApiModelProperty(value = "modifyTime")
    private Date modifyTime;

}
