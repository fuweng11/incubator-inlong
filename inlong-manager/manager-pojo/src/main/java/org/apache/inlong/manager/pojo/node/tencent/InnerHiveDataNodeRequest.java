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

package org.apache.inlong.manager.pojo.node.tencent;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

/**
 * Data node request for inner hive
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.INNER_HIVE)
@ApiModel("Data node request for inner hive")
public class InnerHiveDataNodeRequest extends DataNodeRequest {

    @ApiModelProperty(value = "hive address")
    private String hiveAddress;

    @ApiModelProperty(value = "warehouse dir")
    private String warehouseDir;

    @ApiModelProperty(value = "hdfs default fs")
    private String hdfsDefaultFs;

    @ApiModelProperty(value = "hdfs ugi")
    private String hdfsUgi;

    @ApiModelProperty(value = "cluster tag")
    private String clusterTag;

    @ApiModelProperty(value = "zone id")
    private String zoneId;

    @ApiModelProperty(value = "product id")
    private String productId;

}
