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

package org.apache.inlong.manager.pojo.sort.node.extract;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.oracle.OracleSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.OracleConstant.ScanStartUpMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.OracleExtractNode;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Oracle extract nodes.
 */
public class OracleProvider implements ExtractNodeProvider {

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.ORACLE.equals(sourceType);
    }

    @Override
    public ExtractNode createNode(StreamNode streamNodeInfo) {
        OracleSource source = (OracleSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseFieldInfos(source.getFieldList(), source.getSourceName());
        Map<String, String> properties = parseProperties(source.getProperties());

        ScanStartUpMode scanStartupMode = StringUtils.isBlank(source.getScanStartupMode())
                ? null
                : ScanStartUpMode.forName(source.getScanStartupMode());
        return new OracleExtractNode(
                source.getSourceName(),
                source.getSourceName(),
                fieldInfos,
                null,
                properties,
                source.getPrimaryKey(),
                source.getHostname(),
                source.getUsername(),
                source.getPassword(),
                source.getDatabase(),
                source.getSchemaName(),
                source.getTableName(),
                source.getPort(),
                scanStartupMode);
    }
}