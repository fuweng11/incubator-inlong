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

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DbSync heartbeat info
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("DbSync heartbeat info")
public class DbSyncHeartbeat {

    @ApiModelProperty(value = "Agent address")
    private String instance;

    @ApiModelProperty(value = "ServerId of the task, is the ID of data_node table")
    private String serverId;

    @ApiModelProperty(value = "Currently collected DB")
    private String currentDb;

    @ApiModelProperty(value = "URL of the DB server, such as 127.0.0.1:3306")
    private String url;

    @ApiModelProperty(value = "URL of the standby DB server")
    private String backupUrl;

    @ApiModelProperty(value = "Agent running status, NORMAL, STOPPED, SWITCHED...")
    private String agentStatus;

    @ApiModelProperty(value = "Task IDs being collected by DbSync, is the ID of stream_source table")
    private List<Integer> taskIds;

    @ApiModelProperty(value = "BinLog index currently collected")
    private Long dumpIndex;

    @ApiModelProperty(value = "BinLog position currently collected, will be saved as JSON string")
    private DbSyncDumpPosition dumpPosition;

    private DbSyncDumpPosition sendPosition;

    @ApiModelProperty(value = "BinLog maximum position of the current DB, will be saved as a JSON string")
    private DbSyncDumpPosition maxLogPosition;

    @ApiModelProperty(value = "Error message")
    private String errorMsg;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Long reportTime;

}
