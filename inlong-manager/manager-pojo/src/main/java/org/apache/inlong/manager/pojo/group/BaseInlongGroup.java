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

package org.apache.inlong.manager.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The base parameter class of InlongGroup, support user extend their own business params.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base info of inlong group")
public class BaseInlongGroup {

    // you can add extend parameters in this class
    @ApiModelProperty("product id")
    private Integer productId;

    @ApiModelProperty("product name")
    private String productName;

    @ApiModelProperty("app group name")
    private String appGroupName;

}
