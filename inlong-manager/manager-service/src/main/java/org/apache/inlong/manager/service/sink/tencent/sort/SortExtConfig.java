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

package org.apache.inlong.manager.service.sink.tencent.sort;

import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.ConsistencyGuarantee;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HiveFileFormatInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.PartitionCreationStrategy;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Extended information of sort task
 */
@Data
@NoArgsConstructor
@ApiModel("Extended information of sort task")
public class SortExtConfig {

    @ApiModelProperty("backup data path")
    private String backupDataPath;

    @ApiModelProperty("backup hadoop proxy user")
    private String backupHadoopProxyUser;

    @ApiModelProperty("format info")
    private HiveFileFormatInfo formatInfo;

    @ApiModelProperty("creation strategy")
    private PartitionCreationStrategy creationStrategy;

    @ApiModelProperty("consistency")
    private ConsistencyGuarantee consistency;

}
