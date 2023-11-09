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
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.sink.SinkField;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonTypeDefine(value = SinkType.INNER_ICEBERG)
public class InnerIcebergFieldInfo extends SinkField {

    @ApiModelProperty("Iceberg partition strategy")
    private String partitionStrategy;

    @ApiModelProperty("Iceberg field length")
    private Integer fieldLength;

    @ApiModelProperty("Iceberg field precision")
    private Integer fieldPrecision;

    @ApiModelProperty("Iceberg field scale")
    private Integer fieldScale;

    @ApiModelProperty("Required")
    private Boolean optional = true;

    /**
     * Get the dto instance from the request
     */
    public static InnerIcebergFieldInfo getFromRequest(SinkField sinkField) {
        return CommonBeanUtils.copyProperties(sinkField, InnerIcebergFieldInfo::new, true);
    }

    /**
     * Get the extra param from the Json
     */
    public static InnerIcebergFieldInfo getFromJson(@NotNull String extParams) {
        if (StringUtils.isEmpty(extParams)) {
            return new InnerIcebergFieldInfo();
        }
        try {
            return JsonUtils.parseObject(extParams, InnerIcebergFieldInfo.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("Failed to parse extParams for Iceberg fieldInfo: %s", e.getMessage()));
        }
    }

}
