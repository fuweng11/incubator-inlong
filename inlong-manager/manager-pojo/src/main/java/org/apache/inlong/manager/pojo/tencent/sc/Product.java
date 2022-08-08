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
import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * product info
 */
@Data
@ApiModel("product info")
public class Product {

    @ApiModelProperty("product id")
    private Integer id;

    @ApiModelProperty("product name")
    private String name;

    @ApiModelProperty("product abbreviation")
    private String abbrevName;

    @ApiModelProperty("belonging OBS product id")
    private Integer obsProductId;

    @ApiModelProperty("belonging OBS product name")
    private String obsProductName;

    @ApiModelProperty("belonging to bg id")
    private Integer bgId;

    @ApiModelProperty("belonging to bg name")
    private String bgName;

    @ApiModelProperty("belonging to department id")
    private Integer departmentId;

    @ApiModelProperty("belonging to department name")
    private String department;

    @ApiModelProperty("cost manager list")
    private List<String> costManagers = Lists.newArrayList();

    @ApiModelProperty("product owner")
    private List<String> managers = Lists.newArrayList();

    @ApiModelProperty("descriptive information")
    private String description;

    @ApiModelProperty("create time")
    private Date createTime;

    @ApiModelProperty("update time")
    private Date updateTime;
}
