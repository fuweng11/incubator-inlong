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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Protocol switch info
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Protocol switch info")
public class ProtocolSwitchInfo {

    @ApiModelProperty("Switch status. 1 for switching, 2 for replacing, 3 for rollback-ing")
    private Integer status;

    @ApiModelProperty("Switch time")
    private String switchTime;

    @ApiModelProperty("Replace time")
    private String replaceTime;

    @ApiModelProperty("Rollback time")
    private String rollbackTime;
}
