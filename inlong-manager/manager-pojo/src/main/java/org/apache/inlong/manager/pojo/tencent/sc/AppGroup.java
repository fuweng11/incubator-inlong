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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * App group info
 */
@Data
@ApiModel("app group info")
public class AppGroup {

    @ApiModelProperty("app group ID")
    private Integer id;

    @ApiModelProperty("app group name")
    private String name;

    @ApiModelProperty("app group abbreviation")
    private String alias;

    @ApiModelProperty("cluster id")
    private Integer clusterId;

    @ApiModelProperty("cluster name")
    private String clusterName;

    @ApiModelProperty("bg id")
    private Integer bgId;

    @ApiModelProperty("bg name")
    private String bgName;

    @ApiModelProperty("product id")
    private Integer productId;

    @ApiModelProperty("product name")
    private String productName;

    @ApiModelProperty("managers list")
    private List<String> managers;

    @ApiModelProperty("member list")
    private List<String> members;

    @ApiModelProperty("input database")
    private String inputDatabase;

    @ApiModelProperty("output database")
    private String outputDatabase;

    @ApiModelProperty("creat time")
    private Long createTime;

    @ApiModelProperty("update time")
    private Long updateTime;

    @ApiModelProperty("description")
    private String description;
}
