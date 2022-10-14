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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Task info for DbSync
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Task info for DbSync")
public class DbSyncTaskInfo {

    @ApiModelProperty(value = "Task ID, is ID of stream_source table")
    private Integer id;

    @ApiModelProperty(value = "Inlong group ID")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream ID")
    private String inlongStreamId;

    @ApiModelProperty(value = "Parent cluster ID of the current IP belongs")
    private Integer parentId;

    @ApiModelProperty(value = "All nodes IPs of the current cluster")
    private List<String> nodeIps;

    @ApiModelProperty(value = "DB server name, as a grouping for DbSync scheduling")
    private String serverName;

    @ApiModelProperty(value = "Database name")
    private String dbName;

    @ApiModelProperty(value = "Table name, support regular, such as: order_[0-9]{8}$",
            notes = "All table schemas must be the same")
    private String tableName;

    @ApiModelProperty(value = "Binlog data code name, default is UTF-8")
    private String charset;

    @ApiModelProperty(value = "MQ type, including TUBEMQ, PULSAR, KAFKA, etc")
    private String mqType;

    @ApiModelProperty(value = "TubeTopic, or PulsarNamespace")
    private String mqResource;

    @Deprecated
    @ApiModelProperty(value = "Just for PulsarTopic")
    private String streamMqResource;

    @Deprecated
    @ApiModelProperty(value = "Tube cluster URL")
    private String tubeCluster;

    @Deprecated
    @ApiModelProperty(value = "Pulsar cluster URL")
    private String pulsarCluster;

    @ApiModelProperty(value = "Whether to skip the deletion event in binlog, default: 1, skip")
    private Integer skipDelete;

    @ApiModelProperty(value = "Collect from the specified binlog position",
            notes = "Modify it after publishing, and return an empty string if empty")
    private String startPosition;

    @ApiModelProperty(value = "Operate status")
    private Integer status;

    @ApiModelProperty(value = "Version of current config")
    private Integer version;

    @ApiModelProperty(value = "DB server info")
    private DBServerInfo dbServerInfo;

}
