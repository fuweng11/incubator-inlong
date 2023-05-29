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

package org.apache.inlong.manager.pojo.source.tencent.ha;

import org.apache.inlong.common.pojo.agent.dbsync.DbSyncDumpPosition;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * HA binlog source info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "HA binlog source info")
@JsonTypeDefine(value = SourceType.HA_BINLOG)
public class HaBinlogSource extends StreamSource {

    @ApiModelProperty(value = "Database name")
    private String dbName;

    @ApiModelProperty(value = "Table name, support regular, such as: order_[0-9]{8}$", notes = "All table schemas must be the same")
    private String tableName;

    @ApiModelProperty(value = "Binlog data code, default is UTF-8")
    private String charset;

    @ApiModelProperty(value = "Collect from the specified binlog location, "
            + "and modify it after distribution. If it is empty, null will be returned")
    private DbSyncDumpPosition startPositionObj;

    @ApiModelProperty(value = "Whether to skip the deletion event. Default: 1, skip")
    private Integer skipDelete;

    public HaBinlogSource() {
        this.setSourceType(SourceType.HA_BINLOG);
    }

    @Override
    public SourceRequest genSourceRequest() {
        return CommonBeanUtils.copyProperties(this, HaBinlogSourceRequest::new);
    }
}
