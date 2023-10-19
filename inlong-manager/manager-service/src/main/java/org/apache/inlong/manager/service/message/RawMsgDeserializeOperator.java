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
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgBinlogDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgSeaCubeDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.KvDeserializationInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RawMsgDeserializeOperator implements DeserializeOperator {

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.RAW.equals(type);
    }

    @Override
    public List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo,
            byte[] msgBytes, Map<String, String> headers, int index) {
        String groupId = headers.get(AttributeConstants.GROUP_ID);
        String streamId = headers.get(AttributeConstants.STREAM_ID);
        long msgTime = Long.parseLong(headers.getOrDefault(MSG_TIME_KEY, "0"));
        return Collections.singletonList(new BriefMQMessage(index, groupId, streamId, msgTime,
                headers.get(CLIENT_IP), new String(msgBytes, Charset.forName(streamInfo.getDataEncoding()))));
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
                // need to delete the first separator? default is false
                deserializationInfo = new CsvDeserializationInfo(separator, escape);
                break;
            case TencentConstants.DATA_TYPE_KV:
                deserializationInfo = new KvDeserializationInfo(separator, escape);
                break;
            case TencentConstants.DATA_TYPE_SEA_CUBE:
                deserializationInfo = new InlongMsgSeaCubeDeserializationInfo(streamId);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("current type unsupported data type for warpType=%s, dataType=%s ", wrapType,
                                dataType));
        }
        return deserializationInfo;
    }

}
