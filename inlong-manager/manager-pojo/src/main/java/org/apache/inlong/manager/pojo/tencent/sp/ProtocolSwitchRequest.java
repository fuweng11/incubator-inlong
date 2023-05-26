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

package org.apache.inlong.manager.pojo.tencent.sp;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.Range;

import javax.validation.constraints.NotBlank;

/**
 * Protocol switch request.
 */
@Data
@ApiModel("Protocol switch request")
public class ProtocolSwitchRequest {

    @ApiModelProperty("Inlong group id")
    @NotBlank(message = "inlongGroupId cannot be blank")
    private String inlongGroupId;

    @ApiModelProperty("Inlong stream id")
    @NotBlank(message = "inlongStreamId cannot be blank")
    private String inlongStreamId;

    @ApiModelProperty("Switch hive status. 1 for switching, 2 for replacing, 3 for rollback-ing")
    private Integer status;

    @ApiModelProperty("Switch thive status. 1 for switching, 2 for replacing, 3 for rollback-ing")
    private Integer thStatus;

    @ApiModelProperty("Switch hive time")
    @Length(min = 14, max = 14, message = "switchTime only support yyyyMMddHHmmss format")
    private String switchTime;

    @ApiModelProperty("Switch thive time")
    @Length(min = 14, max = 14, message = "switchTime only support yyyyMMddHHmmss format")
    private String thSwitchTime;

    @ApiModelProperty("Replace hive time")
    @Length(min = 14, max = 14, message = "replaceTime only support yyyyMMddHHmmss format")
    private String replaceTime;

    @ApiModelProperty("Replace thive time")
    @Length(min = 14, max = 14, message = "replaceTime only support yyyyMMddHHmmss format")
    private String thReplaceTime;

    @ApiModelProperty("Rollback hive time")
    @Length(min = 14, max = 14, message = "rollbackTime only support yyyyMMddHHmmss format")
    private String rollbackTime;

    @ApiModelProperty("Rollback thive time")
    @Length(min = 14, max = 14, message = "rollbackTime only support yyyyMMddHHmmss format")
    private String thRollbackTime;

    @ApiModelProperty("zookeeper root path")
    private String zkRootPath;

    @ApiModelProperty("hdfs table path")
    private String hdfsTablePath;

}
