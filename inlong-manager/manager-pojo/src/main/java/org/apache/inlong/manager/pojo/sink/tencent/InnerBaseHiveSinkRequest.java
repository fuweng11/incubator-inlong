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

package org.apache.inlong.manager.pojo.sink.tencent;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.pojo.sink.SinkRequest;

import javax.validation.constraints.NotNull;

/**
 * Inner base hive sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Inner base hive sink request")
public abstract class InnerBaseHiveSinkRequest extends SinkRequest {

    @ApiModelProperty("us task id")
    private String usTaskId;

    @ApiModelProperty(value = "verified task id", notes = "subtasks of the above tasks")
    private String verifiedTaskId;

    @ApiModelProperty("user with select permission by default, multiple are separated by half width commas")
    private String defaultSelectors;

    @NotNull(message = "dbName cannot be null")
    @ApiModelProperty("target database name")
    private String dbName;

    @NotNull(message = "tableName cannot be null")
    @ApiModelProperty("target table name")
    private String tableName;

    @ApiModelProperty("hdfs location")
    private String location;

    @ApiModelProperty("partition type, only for thive:LIST,RANGE")
    private String partitionType = "LIST";

    @ApiModelProperty("partition interval, 1 day, 1 hour, 30 minutes, 10 minutes")
    private Integer partitionInterval = 1;

    @ApiModelProperty("partition units: D-Day, H-Hour, i-minute")
    private String partitionUnit = "D";

    @ApiModelProperty("first level partition field")
    private String primaryPartition;

    @ApiModelProperty("secondary partition")
    private String secondaryPartition;

    @NotNull(message = "partitionCreationStrategy cannot be null")
    @ApiModelProperty(value = "Partition creation policy", notes = "like: ARRIVED-data arrival, COMPLETED-data completion,"
            + "AGENT_COUNT_VERIFIED-data count verification passed,"
            + " DATA_DISTINCT_VERIFIED-data distinct verification passed")
    private String partitionCreationStrategy;

    @NotNull(message = "fileFormat cannot be null")
    @ApiModelProperty("stored table format, like: TextFile, ORCFile, Parquet, etc")
    private String fileFormat;

    @ApiModelProperty("compression type, like: gzip, lzo, etc")
    private String compressionType;

    @ApiModelProperty("Pb file storage path")
    private String filePath;

    @NotNull(message = "dataEncoding cannot be null")
    @ApiModelProperty("data code, UTF-8 by default")
    private String dataEncoding;

    @NotNull(message = "dataSeparator cannot be null")
    @ApiModelProperty("dara separator")
    private String dataSeparator;

    @ApiModelProperty("the responsible person of the library table is the designated virtual user")
    private String virtualUser;

    @ApiModelProperty("data consistency, like: EXACTLY_ONCE, AT_LEAST_ONCE")
    private String dataConsistency;

    @ApiModelProperty(value = "absolute error, like: 0-100:1,1000:10,"
            + "less than 100 pieces of data can be 1 piece less, "
            + " and more than 1000 pieces of data can be 10 pieces less", notes = "the interval is left closed and right open, and cannot be overlapped")
    private String checkAbsolute;

    @ApiModelProperty(value = "relative error, like: 0-100:0.1,1000+:0.02, "
            + "less than 100 pieces of data can be reduced by 0.1%, "
            + "and more than 1000 pieces of data can be reduced by 0.02%", notes = "the interval is left closed and right open, and cannot be overlapped")
    private String checkRelative;

    @ApiModelProperty("background operation log")
    private String optLog;

}
