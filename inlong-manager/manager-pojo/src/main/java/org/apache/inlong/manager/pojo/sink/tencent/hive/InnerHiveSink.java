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

package org.apache.inlong.manager.pojo.sink.tencent.hive;

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

@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Hive sink info")
@JsonTypeDefine(value = SinkType.INNER_HIVE)
public class InnerHiveSink extends StreamSink {

    @ApiModelProperty("is thive")
    private Integer isThive;

    @ApiModelProperty("cluster id")
    private Integer clusterId;

    @ApiModelProperty("us task id")
    private String usTaskId;

    @ApiModelProperty("verified task id")
    private String verifiedTaskId; // the US task of verifying data is a sub task of the above task

    @ApiModelProperty("app group name")
    private String appGroupName;

    @ApiModelProperty("default selectors")
    private String defaultSelectors;

    @ApiModelProperty("database name")
    private String dbName;

    @ApiModelProperty("table name")
    private String tableName;

    @ApiModelProperty("partition type")
    private String partitionType;

    @ApiModelProperty("partition interval")
    private Integer partitionInterval;

    @ApiModelProperty("partition unit")
    private String partitionUnit;

    @ApiModelProperty("primary partition")
    private String primaryPartition;

    @ApiModelProperty("secondary partition")
    private String secondaryPartition;

    @ApiModelProperty("partition creation strategy")
    private String partitionCreationStrategy;

    @ApiModelProperty("file format")
    private String fileFormat;

    @ApiModelProperty("data encoding")
    private String dataEncoding;

    @ApiModelProperty("target separator")
    private String targetSeparator;

    @ApiModelProperty("status")
    private Integer status;

    @ApiModelProperty("creator")
    private String creator;

    // Hive advanced options
    @ApiModelProperty("virtual user")
    private String virtualUser; // the responsible person of the library table is the designated virtual user
    @ApiModelProperty("data consistency")
    private String dataConsistency;

    @ApiModelProperty("check absolute")
    private String checkAbsolute; // absolute error

    @ApiModelProperty("checkout relative")
    private String checkRelative; // relative error

    // configuration in data flow
    @ApiModelProperty("mq resource obj")
    private String mqResourceObj;

    @ApiModelProperty("data source type")
    private String dataSourceType;

    @ApiModelProperty("data type")
    private String dataType;

    @ApiModelProperty("description")
    private String description;

    @ApiModelProperty("source sepatator")
    private String sourceSeparator; // source separator in data flow

    @ApiModelProperty("kv separator")
    private String kvSeparator; // KV separator

    @ApiModelProperty("line separator")
    private String lineSeparator; // line separator

    @ApiModelProperty("data escape char")
    private String dataEscapeChar; // data escape char

    // Hive cluster configuration
    @ApiModelProperty("hive address")
    private String hiveAddress;

    @ApiModelProperty("username")
    private String username;

    @ApiModelProperty("password")
    private String password;

    @ApiModelProperty("warehouse dir")
    private String warehouseDir;

    @ApiModelProperty("hdfs default fs")
    private String hdfsDefaultFs;

    @ApiModelProperty("hdfs ugi")
    private String hdfsUgi;

    @ApiModelProperty("cluster tag")
    private String clusterTag;

    public InnerHiveSink() {
        this.setSinkType(SinkType.INNER_HIVE);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, InnerHiveSinkRequest::new);
    }

}
