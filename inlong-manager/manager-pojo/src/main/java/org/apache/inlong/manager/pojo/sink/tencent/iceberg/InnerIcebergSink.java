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

package org.apache.inlong.manager.pojo.sink.tencent.iceberg;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Iceberg sink info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Iceberg sink info")
@JsonTypeDefine(value = SinkType.INNER_ICEBERG)
public class InnerIcebergSink extends StreamSink {

    @ApiModelProperty("database name")
    private String dbName;

    @ApiModelProperty("table name")
    private String tableName;

    @ApiModelProperty("append mode, UPSERT or APPEND")
    private String appendMode;

    @ApiModelProperty("Table primary key")
    private String primaryKey;

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

    @ApiModelProperty("product id")
    private Integer productId;

    @ApiModelProperty("bg id")
    private Integer bgId; // use for wedata

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, InnerIcebergSinkRequest::new);
    }
}
