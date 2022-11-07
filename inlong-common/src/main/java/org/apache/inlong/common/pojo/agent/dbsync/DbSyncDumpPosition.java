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

/**
 * DbSync backup information, is part of the heartbeat information
 */
@Data
@ApiModel("DbSync backup information")
@AllArgsConstructor
@NoArgsConstructor
public class DbSyncDumpPosition {

    @ApiModelProperty(value = "Database to which BinLog belongs")
    private LogIdentity logIdentity;

    @ApiModelProperty(value = "Entry position of BinLog")
    private EntryPosition entryPosition;

    @Data
    @ApiModel("DB info to which BinLog belongs")
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LogIdentity {

        @ApiModelProperty(value = "DB IP")
        private String sourceIp;

        @ApiModelProperty(value = "DB port")
        private Integer sourcePort;

        @ApiModelProperty(value = "ID of DB server")
        private Integer slaveId;

        @ApiModelProperty(value = "DB server name - is stream_source's data_node_name")
        private String serverName;
    }

    @Data
    @ApiModel("BinLog position info")
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EntryPosition {

        @ApiModelProperty(value = "BinLog file name")
        private String journalName;

        @ApiModelProperty(value = "Position in BinLog file")
        private String position; //TODO: check string or int

        @ApiModelProperty(value = "Whether to include the current position")
        private Boolean included;

        @ApiModelProperty(value = "BinLog timestamp")
        private Long timestamp;
    }
}
