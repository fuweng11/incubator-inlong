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

package org.apache.inlong.manager.pojo.cluster.tencent.zk;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * ZK cluster info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("ZK cluster info")
public class ZkClusterDTO {

    @ApiModelProperty(value = "TubeMQ root")
    private String tubeRoot;

    @ApiModelProperty(value = "Pulsar root")
    private String pulsarRoot;

    @ApiModelProperty(value = "Kafka root")
    private String kafkaRoot;

    /**
     * Get the dto instance from the request
     */
    public static ZkClusterDTO getFromRequest(ZkClusterRequest request) {
        return ZkClusterDTO.builder()
                .tubeRoot(request.getTubeRoot())
                .pulsarRoot(request.getPulsarRoot())
                .kafkaRoot(request.getKafkaRoot())
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static ZkClusterDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, ZkClusterDTO.class);
    }

}
