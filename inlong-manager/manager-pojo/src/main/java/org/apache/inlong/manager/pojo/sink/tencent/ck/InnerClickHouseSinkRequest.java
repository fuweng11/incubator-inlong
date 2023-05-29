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

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Inner click house sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Inner click house sink request")
@JsonTypeDefine(value = SinkType.INNER_CK)
public class InnerClickHouseSinkRequest extends SinkRequest {

    @ApiModelProperty("URL of the ClickHouse server")
    private String url;

    @ApiModelProperty("Username of the ClickHouse server")
    private String username;

    @ApiModelProperty("User password of the ClickHouse server")
    private String password;

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

}
