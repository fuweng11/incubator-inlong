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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.tencent.InnerBaseHiveSinkRequest;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Base sink info for inner Hive or THive.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InnerBaseHiveSinkDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("bg id")
    private Integer bgId;

    @ApiModelProperty("product id")
    private Integer productId;

    @ApiModelProperty("product name")
    private String productName;

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

    @ApiModelProperty("Password encrypt version")
    private Integer encryptVersion;

    /**
     * Get the dto instance from the request
     */
    public static InnerBaseHiveSinkDTO getFromRequest(InnerBaseHiveSinkRequest request) throws Exception {
        Integer encryptVersion = AESUtils.getCurrentVersion(null);
        return InnerBaseHiveSinkDTO.builder()
                .productId(request.getProductId())
                .productName(request.getProductName())
                .defaultSelectors(request.getDefaultSelectors())
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .appGroupName(request.getAppGroupName())
                .dataConsistency(request.getDataConsistency())
                .dataEncoding(request.getDataEncoding())
                .targetSeparator(request.getDataSeparator())
                .fileFormat(request.getFileFormat())
                .partitionType(request.getPartitionType())
                .partitionCreationStrategy(request.getPartitionCreationStrategy())
                .partitionInterval(request.getPartitionInterval())
                .partitionUnit(request.getPartitionUnit())
                .primaryPartition(request.getPrimaryPartition())
                .secondaryPartition(request.getSecondaryPartition())
                .encryptVersion(encryptVersion)
                .build();
    }

    /**
     * Get Hive sink info from JSON string
     */
    public static InnerBaseHiveSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, InnerBaseHiveSinkDTO.class);
        } catch (Exception e) {
            System.out.println(e);
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage());
        }
    }

    /**
     * Get Hive table info
     */
    public static InnerHiveFullInfo getHiveFullInfo(InnerBaseHiveSinkDTO innerHiveInfo, SinkInfo sinkInfo,
            InnerBaseHiveDataNodeInfo innerHiveDataNodeInfo) {
        Integer isThive = Objects.equals(sinkInfo.getSinkType(), SinkType.INNER_THIVE) ? 1 : 0;
        return InnerHiveFullInfo.builder()
                .sinkId(sinkInfo.getId())
                .bgId(innerHiveInfo.getBgId())
                .productId(innerHiveInfo.getProductId())
                .productName(innerHiveInfo.getProductName())
                .appGroupName(innerHiveInfo.getAppGroupName())
                .inlongGroupId(sinkInfo.getInlongGroupId())
                .inlongStreamId(sinkInfo.getInlongStreamId())
                .isThive(isThive)
                .usTaskId(innerHiveInfo.getUsTaskId())
                .verifiedTaskId(innerHiveInfo.getVerifiedTaskId())
                .appGroupName(innerHiveInfo.getAppGroupName())
                .defaultSelectors(innerHiveInfo.getDefaultSelectors())
                .dbName(innerHiveInfo.getDbName())
                .tableName(innerHiveInfo.getTableName())
                .partitionType(innerHiveInfo.getPartitionType())
                .partitionInterval(innerHiveInfo.getPartitionInterval())
                .partitionUnit(innerHiveInfo.getPartitionUnit())
                .primaryPartition(innerHiveInfo.getPrimaryPartition())
                .secondaryPartition(innerHiveInfo.getSecondaryPartition())
                .partitionCreationStrategy(innerHiveInfo.getPartitionCreationStrategy())
                .fileFormat(innerHiveInfo.getFileFormat())
                .dataEncoding(innerHiveInfo.getDataEncoding())
                .targetSeparator(innerHiveInfo.getTargetSeparator())
                .virtualUser(innerHiveInfo.getVirtualUser())
                .dataConsistency(innerHiveInfo.getDataConsistency())
                .checkAbsolute(innerHiveInfo.getCheckAbsolute())
                .checkRelative(innerHiveInfo.getCheckRelative())
                .status(innerHiveInfo.getStatus())
                .creator(sinkInfo.getCreator())
                .mqResourceObj(sinkInfo.getMqResource())
                .dataType(sinkInfo.getDataType())
                .sourceSeparator(sinkInfo.getSourceSeparator())
                .dataEscapeChar(sinkInfo.getDataEscapeChar())
                .hiveAddress(innerHiveDataNodeInfo.getHiveAddress())
                .username(innerHiveDataNodeInfo.getUsername())
                .password(innerHiveDataNodeInfo.getToken())
                .warehouseDir(innerHiveDataNodeInfo.getWarehouseDir())
                .hdfsDefaultFs(innerHiveDataNodeInfo.getHdfsDefaultFs())
                .hdfsUgi(innerHiveDataNodeInfo.getHdfsUgi())
                .clusterTag(innerHiveDataNodeInfo.getClusterTag())
                .build();
    }

    private InnerBaseHiveSinkDTO decryptPassword() throws Exception {
        if (StringUtils.isNotEmpty(this.password)) {
            byte[] passwordBytes = AESUtils.decryptAsString(this.password, this.encryptVersion);
            this.password = new String(passwordBytes, StandardCharsets.UTF_8);
        }
        return this;
    }
}
