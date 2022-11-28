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
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * HA binlog source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HaBinlogSourceDTO {

    @ApiModelProperty(value = "Database name")
    private String dbName;

    @ApiModelProperty(value = "Table name, support regular, such as: order_[0-9]{8}$", notes = "All table schemas must be the same")
    private String tableName;

    @ApiModelProperty(value = "Binlog data code, default is UTF-8")
    private String charset;

    @ApiModelProperty(value = "Whether to skip the deletion event. Default: 1, skip")
    private Integer skipDelete;

    /**
     * Get the dto instance from the request
     */
    public static HaBinlogSourceDTO getFromRequest(HaBinlogSourceRequest request) {
        return HaBinlogSourceDTO.builder()
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .charset(request.getCharset())
                // .startPositionObj(request.getStartPositionObj())
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
