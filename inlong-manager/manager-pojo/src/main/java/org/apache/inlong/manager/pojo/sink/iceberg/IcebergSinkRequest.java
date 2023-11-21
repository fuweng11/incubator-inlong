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

package org.apache.inlong.manager.pojo.sink.iceberg;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Iceberg sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Iceberg sink request")
@JsonTypeDefine(value = SinkType.ICEBERG)
public class IcebergSinkRequest extends SinkRequest {

    @ApiModelProperty("Catalog type, like: HIVE, HADOOP, default is HIVE")
    private String catalogType = "HIVE";

    @ApiModelProperty("Catalog uri, such as hive metastore thrift://ip:port")
    private String catalogUri;

    @ApiModelProperty("Iceberg data warehouse dir")
    private String warehouse;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Data path, such as: hdfs://ip:port/user/hive/warehouse/test.db")
    private String dataPath;

    @ApiModelProperty("File format, support: Parquet, Orc, Avro")
    private String fileFormat;

    @ApiModelProperty("Partition type, like: H-hour, D-day, W-week, M-month, O-once, R-regulation")
    private String partitionType;

    @ApiModelProperty("Primary key")
    private String primaryKey;

    @ApiModelProperty("append mode, UPSERT or APPEND")
    private String appendMode;

    @ApiModelProperty("watermark field")
    private String watermarkField;

    @ApiModelProperty("watermark format")
    private String watermarkFormat;

    @ApiModelProperty("cluster tag")
    private String clusterTag;

    @ApiModelProperty("iceberg checker task id")
    private String icebergCheckerTaskId;

    @ApiModelProperty("data pattern")
    private String datePattern;

    @ApiModelProperty("cycle num")
    private String cycleNum;

    @ApiModelProperty("cycle unit")
    private String cycleUnit;

    @ApiModelProperty("gaia id")
    private String gaiaId;

    @ApiModelProperty("resource group")
    private String resourceGroup; // use for wedata

    @ApiModelProperty("bg id")
    private Integer bgId; // use for wedata

}
