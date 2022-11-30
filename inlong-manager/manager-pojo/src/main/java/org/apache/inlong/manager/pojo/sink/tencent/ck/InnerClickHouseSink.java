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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;

/**
 * Hive sink info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Click house sink info")
@JsonTypeDefine(value = SinkType.INNER_CK)
public class InnerClickHouseSink extends StreamSink {

    @ApiModelProperty("URL of the ClickHouse server")
    private String url;

    @ApiModelProperty("Username of the ClickHouse server")
    private String username;

    @ApiModelProperty("User password of the ClickHouse server")
    private String password;

    @ApiModelProperty("app group name")
    private String appGroupName;

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

    @ApiModelProperty("data consistency, including:Actually_ONCE, AT_LEAST_ONCE")
    private String dataConsistency;

    public InnerClickHouseSink() {
        this.setSinkType(SinkType.INNER_CK);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, InnerClickHouseSinkRequest::new);
    }

}
