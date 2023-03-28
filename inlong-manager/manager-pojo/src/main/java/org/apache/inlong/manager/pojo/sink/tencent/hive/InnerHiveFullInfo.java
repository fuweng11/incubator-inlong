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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Hive full configuration information of (used by us and sort)
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class InnerHiveFullInfo {

    private Integer sinkId;
    private Integer bgId;
    private Integer productId;
    private String productName;

    // hive configuration
    private Integer id;
    private String inlongGroupId;
    private String inlongStreamId;
    private Integer isThive;
    private String usTaskId;
    private String usCheckId;
    private String usImportId;
    private String verifiedTaskId; // the US task of verifying data is a sub task of the above task
    private String appGroupName;
    private String defaultSelectors;

    private String dbName;
    private String tableName;
    private String location;
    private String partitionType;
    private Integer partitionInterval;
    private String partitionUnit;

    private String primaryPartition;
    private String secondaryPartition;
    private String partitionCreationStrategy;

    private String fileFormat;
    private String compressionType;
    private String sourceEncoding;
    private String sinkEncoding;
    // target separator configured in data store
    private String targetSeparator;
    private Integer status;
    private String creator;

    // Hive advanced options
    private String virtualUser; // the responsible person of the library table is the designated virtual user
    private String dataConsistency;
    private String checkAbsolute; // absolute error
    private String checkRelative; // relative error

    // configuration in inlong stream
    private String mqResourceObj;
    private String dataType;
    private String description;
    private String sourceSeparator; // source separator in data flow
    private String kvSeparator; // KV separator
    private String lineSeparator; // line separator
    private String dataEscapeChar; // data escape char

    // Hive cluster configuration
    private String hiveAddress;
    private String omsAddress;
    private String username;
    private String password;
    private String warehouseDir;
    private String hdfsDefaultFs;
    private String hdfsUgi;
    private String clusterTag;
    private Integer hadoopDfsReplication;

}