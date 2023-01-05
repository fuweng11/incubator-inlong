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

package org.apache.inlong.manager.pojo.node.tencent.ck;

import io.swagger.annotations.ApiModel;
import lombok.Builder;
import lombok.Data;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * Inner hive data node info
 */
@Data
@Builder
@ApiModel("Inner click house data node info")
public class InnerClickHouseDataNodeDTO {

    /**
     * Get the dto instance from the request
     */
    public static InnerClickHouseDataNodeDTO getFromRequest(InnerClickHouseDataNodeRequest request) throws Exception {
        return InnerClickHouseDataNodeDTO.builder()
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static InnerClickHouseDataNodeDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, InnerClickHouseDataNodeDTO.class);
    }
}