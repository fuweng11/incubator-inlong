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

package org.apache.inlong.manager.pojo.module;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Module request.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Module info")
public class ModuleDTO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleDTO.class);

    @ApiModelProperty("startCommand")
    private String startCommand;

    @ApiModelProperty("stopCommand")
    private String stopCommand;

    @ApiModelProperty("checkCommand")
    private String checkCommand;

    @ApiModelProperty("installCommand")
    private String installCommand;

    @ApiModelProperty("uninstallCommand")
    private String uninstallCommand;

    @ApiModelProperty("History list of package")
    private List<PackageHistory> packageHistoryList;

    /**
     * Get the dto instance from the request
     */
    public static ModuleDTO getFromRequest(ModuleRequest request, String extParams, Integer packageId) {
        ModuleDTO dto = new ModuleDTO();
        if (StringUtils.isNotBlank(extParams)) {
            dto = ModuleDTO.getFromJson(extParams);
            if (!Objects.equals(request.getPackageId(), packageId)) {
                List<PackageHistory> packageHistoryList = dto.getPackageHistoryList();
                if (CollectionUtils.isEmpty(packageHistoryList)) {
                    packageHistoryList = new ArrayList<>();
                } else if (packageHistoryList.size() > 10) {
                    packageHistoryList.remove(packageHistoryList.size() - 1);
                }
                if (packageId != null) {
                    PackageHistory packageHistory = PackageHistory.builder()
                            .packageId(packageId)
                            .modifier(request.getCurrentUser())
                            .modifyTime(new Date())
                            .build();
                    packageHistoryList.add(0, packageHistory);
                }
                dto.setPackageHistoryList(packageHistoryList);
            }
        } else {
            dto.setPackageHistoryList(new ArrayList<>());
        }
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static ModuleDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, ModuleDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    ErrorCodeEnum.CLUSTER_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

}
