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
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.util.StringUtil;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import com.tencent.oceanus.etl.configuration.Constants;
import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgBinlogDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgPbV1DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.InlongMsgSeaCubeDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.KvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class InlongMsgDeserializeOperator implements DeserializeOperator {

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.INLONG_MSG_V0.equals(type);
    }

    @Override
    public List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo, byte[] msgBytes, Map<String, String> headers,
            int index) {
        String groupId = headers.get(AttributeConstants.GROUP_ID);
        String streamId = headers.get(AttributeConstants.STREAM_ID);
        List<BriefMQMessage> messageList = new ArrayList<>();
        InLongMsg inLongMsg = InLongMsg.parseFrom(msgBytes);
        for (String attr : inLongMsg.getAttrs()) {
            Map<String, String> attrMap = StringUtil.splitKv(attr, INLONGMSG_ATTR_ENTRY_DELIMITER,
                    INLONGMSG_ATTR_KV_DELIMITER, null, null);
            // Extracts time from the attributes
            long msgTime;
            if (attrMap.containsKey(INLONGMSG_ATTR_TIME_T)) {
                String date = attrMap.get(INLONGMSG_ATTR_TIME_T).trim();
                msgTime = StringUtil.parseDateTime(date);
            } else if (attrMap.containsKey(INLONGMSG_ATTR_TIME_DT)) {
                String epoch = attrMap.get(INLONGMSG_ATTR_TIME_DT).trim();
                msgTime = Long.parseLong(epoch);
            } else {
                throw new IllegalArgumentException(String.format("PARSE_ATTR_ERROR_STRING%s",
                        INLONGMSG_ATTR_TIME_T + " or " + INLONGMSG_ATTR_TIME_DT));
            }
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            while (iterator.hasNext()) {
                byte[] bodyBytes = iterator.next();
                if (Objects.isNull(bodyBytes)) {
                    continue;
                }
                BriefMQMessage message = new BriefMQMessage(index, groupId, streamId, msgTime, attrMap.get(CLIENT_IP),
                        new String(bodyBytes, Charset.forName(streamInfo.getDataEncoding())));
                messageList.add(message);
            }
        }
        return messageList;
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
                deserializationInfo = new InlongMsgCsvDeserializationInfo(streamId, separator, escape, false);
                break;
            case TencentConstants.DATA_TYPE_TDMSG_CSV:
                deserializationInfo = new TDMsgCsvDeserializationInfo(streamId, separator, escape, false);
                break;
            case TencentConstants.DATA_TYPE_RAW_CSV:
                if (Objects.equals(wrapType, MessageWrapType.RAW.getName())) {
                    deserializationInfo = new CsvDeserializationInfo(separator, escape);
                } else {
                    deserializationInfo = new InlongMsgCsvDeserializationInfo(streamId, separator, escape, false);
                }
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
            case TencentConstants.DATA_TYPE_RAW_KV:
                deserializationInfo = new KvDeserializationInfo(separator, escape);
                break;
            case TencentConstants.DATA_TYPE_INLONG_MSG_V1:
                DeserializationInfo inner = new CsvDeserializationInfo(separator, escape);
                deserializationInfo = new InlongMsgPbV1DeserializationInfo(Constants.CompressionType.GZIP, inner);
                break;
            case TencentConstants.DATA_TYPE_INLONG_MSG_V1_KV:
                DeserializationInfo innerKv = new KvDeserializationInfo(separator, '=', escape);
                deserializationInfo = new InlongMsgPbV1DeserializationInfo(Constants.CompressionType.GZIP, innerKv);
                break;
            case TencentConstants.DATA_TYPE_SEA_CUBE:
                deserializationInfo = new InlongMsgSeaCubeDeserializationInfo(streamId);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("current type unsupported data type for warpType=%s, dataType=%s ",
                                wrapType, dataType));
        }
        return deserializationInfo;
    }
}
