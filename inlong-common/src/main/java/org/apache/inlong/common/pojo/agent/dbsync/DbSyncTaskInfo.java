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

package org.apache.inlong.common.pojo.agent.dbsync;

import org.apache.inlong.common.enums.DataReportTypeEnum;
import org.apache.inlong.common.pojo.dataproxy.DataProxyTopicInfo;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

import static org.apache.inlong.common.enums.DataReportTypeEnum.DIRECT_SEND_TO_MQ;
import static org.apache.inlong.common.enums.DataReportTypeEnum.NORMAL_SEND_TO_DATAPROXY;
import static org.apache.inlong.common.enums.DataReportTypeEnum.PROXY_SEND_TO_DATAPROXY;

/**
 * Task info for DbSync
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Task info for DbSync")
public class DbSyncTaskInfo {

    @ApiModelProperty(value = "Task ID, is ID of stream_source table")
    private Integer id;

    @ApiModelProperty(value = "Inlong group ID")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream ID")
    private String inlongStreamId;

    @ApiModelProperty(value = "Parent cluster ID of the current IP belongs")
    private Integer parentId;

    @ApiModelProperty(value = "All nodes IPs of the current cluster")
    private List<String> nodeIps;

    @ApiModelProperty(value = "DB server name, as a grouping for DbSync scheduling")
    private String serverName;

    @ApiModelProperty(value = "Database name")
    private String dbName;

    @ApiModelProperty(value = "Table name, support regular, such as: order_[0-9]{8}$", notes = "All table schemas must be the same")
    private String tableName;

    @ApiModelProperty(value = "Binlog data code name, default is UTF-8")
    private String charset;

    @ApiModelProperty(value = "Whether to skip the deletion event in binlog, default: 1, skip")
    private Integer skipDelete;

    @ApiModelProperty(value = "Collect from the specified binlog position", notes = "Modify it after publishing, and return an empty string if empty")
    private String startPosition;

    @ApiModelProperty(value = "Operate status")
    private Integer status;

    @ApiModelProperty(value = "Version of current config")
    private Integer version;

    @ApiModelProperty(value = "DB server info")
    private DBServerInfo dbServerInfo;

    /**
     * Data report type.
     * The current constraint is that all InLong Agents under one InlongGroup use the same type.
     * <p/>
     * This constraint is not applicable to InlongStream or StreamSource, which avoids the configuration
     * granularity and reduces the operation and maintenance costs.
     * <p/>
     * Supported type:
     * <pre>
     *     0: report to DataProxy and respond when the DataProxy received data.
     *     1: report to DataProxy and respond after DataProxy sends data.
     *     2: report to MQ and respond when the MQ received data.
     * </pre>
     */
    @ApiModelProperty(value = "Data report type")
    private Integer dataReportType = 0;

    /**
     * MQ cluster information, valid when reportDataTo is 2.
     */
    @ApiModelProperty(value = "mq cluster information")
    private List<MQClusterInfo> mqClusters;

    /**
     * MQ's topic information, valid when reportDataTo is 2.
     */
    @ApiModelProperty(value = "mq topic information")
    private DataProxyTopicInfo topicInfo;

    public boolean isValid() {
        DataReportTypeEnum reportType = DataReportTypeEnum.getReportType(dataReportType);
        if (reportType == NORMAL_SEND_TO_DATAPROXY || reportType == PROXY_SEND_TO_DATAPROXY) {
            return true;
        }
        if (reportType == DIRECT_SEND_TO_MQ && CollectionUtils.isNotEmpty(mqClusters) && mqClusters.stream()
                .allMatch(MQClusterInfo::isValid) && topicInfo.isValid()) {
            return true;
        }
        return false;
    }
}
