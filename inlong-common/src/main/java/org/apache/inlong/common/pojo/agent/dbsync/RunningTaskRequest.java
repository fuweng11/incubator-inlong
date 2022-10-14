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

package org.apache.inlong.common.pojo.agent.dbsync;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Request of DbSync to pull running tasks
 */
@Data
@ApiModel("Request of DbSync to pull running tasks")
public class RunningTaskRequest {

    @NotNull
    @ApiModelProperty(value = "IP of the current node")
    private String ip;

    @NotBlank
    @ApiModelProperty(value = "Cluster tag of the current node")
    private String clusterTag;

    @NotBlank
    @ApiModelProperty(value = "Cluster name of the current node")
    private String clusterName;

    @NotNull
    @ApiModelProperty(value = "Cluster info")
    private DbSyncClusterInfo dbSyncCluster;

    @ApiModelProperty(value = "DB server name")
    private String serverName;

}
