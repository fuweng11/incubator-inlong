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

package org.apache.inlong.manager.pojo.source.tencent.ha;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncDumpPosition;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * Binlog source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HaBinlogSourceDTO {

    @ApiModelProperty(value = "List of DBs to be collected, seperated by ',', supporting regular expressions")
    private String databaseWhiteList;

    @ApiModelProperty(value = "List of tables to be collected, seperated by ',',supporting regular expressions")
    private String tableWhiteList;

    @ApiModelProperty("Binlog data code, default is UTF-8")
    private String charset;

    @ApiModelProperty(value = "Collect from the specified binlog location, "
            + "and modify it after distribution. If it is empty, null will be returned",
            notes = "sourceIp, sourcePort, journalName, position required")
    private DbSyncDumpPosition startDumpPosition;

    @ApiModelProperty(value = "Whether to skip the deletion event. Default: 1, skip")
    private Integer skipDelete;

    /**
     * Get the dto instance from the request
     */
    public static HaBinlogSourceDTO getFromRequest(HaBinlogSourceRequest request) {
        return HaBinlogSourceDTO.builder()
                .databaseWhiteList(request.getDatabaseWhiteList())
                .tableWhiteList(request.getTableWhiteList())
                .charset(request.getCharset())
                .startDumpPosition(request.getStartDumpPosition())
                .skipDelete(request.getSkipDelete())
                .build();
    }

    public static HaBinlogSourceDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, HaBinlogSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }
}
