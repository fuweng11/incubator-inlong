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

package org.apache.inlong.manager.service.message;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgBinlogDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TDMsgDeserializeOperator implements DeserializeOperator {

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.TDMSG1.equals(type);
    }

    @Override
    public DeserializationInfo getDeserializationInfo(InlongStreamInfo streamInfo) {
        String dataType = streamInfo.getDataType();
        Character escape = null;
        DeserializationInfo deserializationInfo;
        if (streamInfo.getDataEscapeChar() != null) {
            escape = streamInfo.getDataEscapeChar().charAt(0);
        }

        String streamId = streamInfo.getInlongStreamId();
        char separator = 0;
        if (StringUtils.isNotBlank(streamInfo.getDataSeparator())) {
            separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
        }
        String wrapType = streamInfo.getWrapType();
        switch (dataType) {
            case TencentConstants.DATA_TYPE_BINLOG:
                deserializationInfo = new InlongMsgBinlogDeserializationInfo(streamId);
                break;
            case TencentConstants.DATA_TYPE_CSV:
                deserializationInfo = new TDMsgCsvDeserializationInfo(streamId, separator, escape, false);
                break;
            case TencentConstants.DATA_TYPE_KV:
                // KV pair separator, which must be the field separator in the data flow
                // TODO should get from the user defined
                char kvSeparator = '&';
                if (StringUtils.isNotBlank(streamInfo.getKvSeparator())) {
                    kvSeparator = (char) Integer.parseInt(streamInfo.getKvSeparator());
                }
                // row separator, which must be a field separator in the data flow
                Character lineSeparator = null;
                if (StringUtils.isNotBlank(streamInfo.getLineSeparator())) {
                    lineSeparator = (char) Integer.parseInt(streamInfo.getLineSeparator());
                }
                // TODO The Sort module need to support
                deserializationInfo = new TDMsgKvDeserializationInfo(streamId, separator, kvSeparator,
                        escape, lineSeparator);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("current type unsupported data type for warpType=%s, dataType=%s ",
                                wrapType, dataType));
        }
        return deserializationInfo;
    }
}
