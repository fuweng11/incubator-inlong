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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;

import java.util.List;

/**
 * Request of DbSync to report results and pull tasks
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Request of DbSync to report results and pull tasks")
public class ReportTaskRequest {

    @NotBlank
    @ApiModelProperty(value = "IP of the current node")
    private String ip;

    @NotBlank
    @ApiModelProperty(value = "Cluster tag of the current node")
    private String clusterTag;

    @NotBlank
    @ApiModelProperty(value = "Cluster name of the current node")
    private String clusterName;

    @NotBlank
    @ApiModelProperty(value = "DbSync cluster info")
    private DbSyncClusterInfo dbSyncCluster;

    @ApiModelProperty(value = "Server names running on the current node")
    private List<String> serverNames;

    @ApiModelProperty(value = "Task info list")
    private List<TaskInfoBean> taskInfoList;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ApiModel("Task result, corresponding to the stream_source table")
    public static class TaskInfoBean {

        @ApiModelProperty(value = "Current source ID")
        private Integer id;

        @ApiModelProperty(value = "Agent status")
        private Integer status;

        @ApiModelProperty(value = "Task execution msg")
        private String message;

        @ApiModelProperty(value = "Task processing result, 0: success, 1: failure, -1: scheduling failure")
        private Integer result;

        @ApiModelProperty(value = "Version of task config - related to the version of stream_source")
        private Integer version;

        @Override
        public String toString() {
            return id + "-" + version + "-" + status + "-" + message + "-" + result;
        }
    }

}
