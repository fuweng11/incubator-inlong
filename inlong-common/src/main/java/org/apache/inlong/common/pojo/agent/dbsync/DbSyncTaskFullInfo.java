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

import java.util.List;

/**
 * Full task info, including new/deleted ServerNames
 */
@Data
@ApiModel("Full task info")
public class DbSyncTaskFullInfo {

    @ApiModelProperty(value = "DbSync cluster info")
    private DbSyncClusterInfo cluster;

    @Deprecated
    @ApiModelProperty(value = "List of offline ServerIDs, replaced by offlineServers")
    private List<Integer> offlineServerIdList;

    @ApiModelProperty(value = "Offline server name list")
    private List<String> offlineServers;

    @Deprecated
    @ApiModelProperty(value = "List of changed ServerIDs, replaced by changedServers")
    private List<Integer> changedServerIdList;

    @ApiModelProperty(value = "Changed server name list")
    private List<String> changedServers;

    @ApiModelProperty(value = "Task info list")
    private List<DbSyncTaskInfo> taskInfoList;

}
