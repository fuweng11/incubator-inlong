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

package org.apache.inlong.manager.pojo.sink.tencent.ck;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * Inner click house sink info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InnerClickHouseDTO {

    @ApiModelProperty("target database name")
    private String dbName;

    @ApiModelProperty("target table name")
    private String tableName;

    @ApiModelProperty("flush interval, unit: second, default is 1s")
    private Integer flushInterval;

    @ApiModelProperty("number of packages, default: 1000")
    private Integer packageSize;

    @ApiModelProperty("retry times, default: 3 times")
    private Integer retryTime;

    @ApiModelProperty("whether it is a distributed table, 0: No, 1: Yes")
    private Integer isDistribute;

    @ApiModelProperty("partition strategy, including: balance, random, hash")
    private String partitionStrategy;

    @ApiModelProperty("partition field, supporting multiple, separated by half width commas")
    private String partitionFields;

    @ApiModelProperty("data consistency, including:Actually_ONCE,AT_LEAST_ONCE")
    private String dataConsistency;

    /**
     * Get the dto instance from the request
     */
    public static InnerClickHouseDTO getFromRequest(InnerClickHouseSinkRequest request) {
        return InnerClickHouseDTO.builder()
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .flushInterval(request.getFlushInterval())
                .packageSize(request.getPackageSize())
                .retryTime(request.getRetryTime())
                .isDistribute(request.getIsDistribute())
                .partitionStrategy(request.getPartitionStrategy())
                .partitionFields(request.getPartitionFields())
                .dataConsistency(request.getDataConsistency())
                .build();
    }

    public static InnerClickHouseDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, InnerClickHouseDTO.class);
    }
}
