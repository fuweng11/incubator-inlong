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

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.tencent.InnerBaseHiveSinkRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

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

    @ApiModelProperty("bg id")
    private Integer bgId;

    @ApiModelProperty("us task id")
    private String usTaskId;

    @ApiModelProperty(value = "us check task id", notes = "use for the under switching group")
    private String usCheckTaskId;

    @ApiModelProperty(value = "us import task id", notes = "use for the under switching group")
    private String usImportTaskId;

    @ApiModelProperty("verified task id")
    private String verifiedTaskId; // the US task of verifying data is a sub-task of the above task

    @ApiModelProperty("default selectors")
    private String defaultSelectors;

    @ApiModelProperty("database name")
    private String dbName;

    @ApiModelProperty("table name")
    private String tableName;

    @ApiModelProperty("hdfs location")
    private String location;

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

    @ApiModelProperty("compression type")
    private String compressionType;

    @ApiModelProperty("data encoding")
    private String dataEncoding;

    @ApiModelProperty("data separator")
    private String dataSeparator;

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

    @ApiModelProperty("data type")
    private String dataType;

    @ApiModelProperty("description")
    private String description;

    @ApiModelProperty("source separator")
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

    @ApiModelProperty("Hadoop dfs replication ")
    private Integer hadoopDfsReplication;

    @ApiModelProperty("Inlong reconciliation type")
    private String reconciliationType = "InlongCft";

    /**
     * Get the dto instance from the request
     */
    public static InnerBaseHiveSinkDTO getFromRequest(InnerBaseHiveSinkRequest request, String extParams)
            throws Exception {
        InnerBaseHiveSinkDTO dto = StringUtils.isNotBlank(extParams)
                ? InnerBaseHiveSinkDTO.getFromJson(extParams)
                : new InnerBaseHiveSinkDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get Hive sink info from JSON string
     */
    public static InnerBaseHiveSinkDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, InnerBaseHiveSinkDTO.class);
    }

    /**
     * Get Hive table info
     */
    public static InnerHiveFullInfo getFullInfo(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            InnerBaseHiveSinkDTO innerHiveDTO, SinkInfo sinkInfo,
            InnerBaseHiveDataNodeInfo hiveDataNode) throws Exception {
        Integer hadoopDfsReplication =
                Objects.isNull(innerHiveDTO.getHadoopDfsReplication()) ? 2 : innerHiveDTO.getHadoopDfsReplication();
        Integer isThive = Objects.equals(sinkInfo.getSinkType(), SinkType.INNER_THIVE) ? 1 : 0;
        String password = encryptPassword(hiveDataNode.getToken(), innerHiveDTO.getTableName(),
                hiveDataNode.getOmsAddress());
        return InnerHiveFullInfo.builder()
                .sinkId(sinkInfo.getId())
                .productId(groupInfo.getProductId())
                .productName(groupInfo.getProductName())
                .appGroupName(groupInfo.getAppGroupName())
                .bgId(innerHiveDTO.getBgId())
                .inlongGroupId(sinkInfo.getInlongGroupId())
                .inlongStreamId(sinkInfo.getInlongStreamId())
                .isThive(isThive)
                .reconciliationType(innerHiveDTO.getReconciliationType())
                .usTaskId(innerHiveDTO.getUsTaskId())
                .usCheckId(innerHiveDTO.getUsCheckTaskId())
                .usImportId(innerHiveDTO.getUsImportTaskId())
                .verifiedTaskId(innerHiveDTO.getVerifiedTaskId())
                .defaultSelectors(innerHiveDTO.getDefaultSelectors())
                .dbName(innerHiveDTO.getDbName())
                .tableName(innerHiveDTO.getTableName())
                .partitionType(innerHiveDTO.getPartitionType())
                .partitionInterval(innerHiveDTO.getPartitionInterval())
                .partitionUnit(innerHiveDTO.getPartitionUnit())
                .primaryPartition(innerHiveDTO.getPrimaryPartition())
                .secondaryPartition(innerHiveDTO.getSecondaryPartition())
                .partitionCreationStrategy(innerHiveDTO.getPartitionCreationStrategy())
                .fileFormat(innerHiveDTO.getFileFormat())
                .compressionType(innerHiveDTO.getCompressionType())
                .sinkEncoding(innerHiveDTO.getDataEncoding())
                .sourceEncoding(streamInfo.getDataEncoding())
                .targetSeparator(innerHiveDTO.getDataSeparator())
                .virtualUser(innerHiveDTO.getVirtualUser())
                .dataConsistency(innerHiveDTO.getDataConsistency())
                .checkAbsolute(innerHiveDTO.getCheckAbsolute())
                .checkRelative(innerHiveDTO.getCheckRelative())
                .status(innerHiveDTO.getStatus())
                .creator(sinkInfo.getCreator())
                .mqResourceObj(sinkInfo.getMqResource())
                .dataType(sinkInfo.getDataType())
                .sourceSeparator(sinkInfo.getSourceSeparator())
                .dataEscapeChar(sinkInfo.getDataEscapeChar())
                .hiveAddress(hiveDataNode.getHiveAddress())
                .omsAddress(hiveDataNode.getOmsAddress())
                .username(hiveDataNode.getUsername())
                .password(password)
                .warehouseDir(hiveDataNode.getWarehouseDir())
                .hdfsDefaultFs(hiveDataNode.getHdfsDefaultFs())
                .hdfsUgi(hiveDataNode.getHdfsUgi())
                .clusterTag(hiveDataNode.getClusterTag())
                .hadoopDfsReplication(hadoopDfsReplication)
                .build();
    }

    private static String encryptPassword(String password, String key, String omsAddress) throws Exception {
        if (StringUtils.isBlank(omsAddress)) {
            return password;
        }
        byte[] passwordBytes = Base64.encodeBase64(password.getBytes(StandardCharsets.UTF_8), false);
        byte[] cipheredBytes = AESUtils.encrypt(passwordBytes, key.trim().getBytes(StandardCharsets.UTF_8));
        return AESUtils.parseByte2HexStr(cipheredBytes);
    }

    private InnerBaseHiveSinkDTO decryptPassword() throws Exception {
        if (StringUtils.isNotEmpty(this.password)) {
            byte[] passwordBytes = AESUtils.decryptAsString(this.password, this.encryptVersion);
            this.password = new String(passwordBytes, StandardCharsets.UTF_8);
        }
        return this;
    }
}
