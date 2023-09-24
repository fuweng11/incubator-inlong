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

package org.apache.inlong.dataproxy.sink2;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.dataproxy.base.SinkRspEvent;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;
import org.apache.inlong.dataproxy.source.ServerMessageHandler;
import org.apache.inlong.dataproxy.utils.DateTimeUtils;
import org.apache.inlong.sdk.commons.protocol.EventConstants;

import com.tencent.tubemq.corebase.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Event extend class
 */
public class EventProfile {

    private static final Logger logger = LoggerFactory.getLogger(SimplePackProfile.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);

    private boolean bringEvent;
    private boolean isIndex;
    private Message message;
    private Event event;
    private int retries;
    // cache information
    private boolean parsedFields = false;
    private boolean isValidIndexMsg = false;
    private boolean isStatusIndex = false;
    private boolean needRspEvent = false;
    private boolean needAck = true;
    private String groupId;
    private String streamId;
    private String srcTopic;
    private String uniqId;
    private String msgSeqId;
    private long dt;
    private int msgCnt;
    private long msgSize;
    private long pkgTime;
    private String dataProxyIp;
    private String clientIp;
    private Channel channel;
    private MsgType msgType;

    public EventProfile(Event event, boolean isIndex) {
        this.isIndex = isIndex;
        this.bringEvent = true;
        this.event = event;
        this.message = null;
        this.groupId = event.getHeaders().get(AttributeConstants.GROUP_ID);
        this.msgSize = event.getBody().length;
    }

    public EventProfile(EventProfile that, Message message) {
        this.bringEvent = false;
        this.event = null;
        this.message = message;
        this.isIndex = that.isIndex;
        this.isValidIndexMsg = that.isValidIndexMsg;
        this.isStatusIndex = that.isStatusIndex;
        this.clientIp = that.clientIp;
        this.groupId = that.groupId;
        this.streamId = that.streamId;
        this.srcTopic = that.srcTopic;
        this.uniqId = that.uniqId;
        this.msgSeqId = that.msgSeqId;
        this.dt = that.dt;
        this.msgSize = that.msgSize;
        this.retries = that.retries;
        this.parsedFields = that.parsedFields;
        this.needRspEvent = that.needRspEvent;
        this.needAck = that.needAck;
        this.pkgTime = that.pkgTime;
        this.msgCnt = that.msgCnt;
        this.dataProxyIp = that.dataProxyIp;
        this.channel = null;
        this.msgType = null;
    }

    public void parseFields() {
        if (!bringEvent || parsedFields) {
            return;
        }
        this.retries = 0;
        this.streamId = event.getHeaders().get(AttributeConstants.STREAM_ID);
        this.srcTopic = event.getHeaders().get(ConfigConstants.TOPIC_KEY);
        this.uniqId = event.getHeaders().getOrDefault(AttributeConstants.UNIQ_ID, "");
        this.dt = NumberUtils.toLong(event.getHeaders().get(AttributeConstants.DATA_TIME));
        this.msgSeqId = event.getHeaders().get(ConfigConstants.SEQUENCE_ID);
        this.pkgTime = Long.parseLong(event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));
        this.msgCnt = NumberUtils.toInt(event.getHeaders().get(ConfigConstants.MSG_COUNTER_KEY), 1);
        this.clientIp = event.getHeaders().get(ConfigConstants.REMOTE_IP_KEY);
        this.dataProxyIp = event.getHeaders().get(ConfigConstants.DATAPROXY_IP_KEY);
        if (this.isIndex) {
            String indexMsgType = event.getHeaders().get(ConfigConstants.INDEX_MSG_TYPE);
            if (ConfigConstants.INDEX_TYPE_FILE_STATUS.equals(indexMsgType)
                    || ConfigConstants.INDEX_TYPE_MEASURE.equals(indexMsgType)) {
                this.isValidIndexMsg = true;
                this.isStatusIndex = ConfigConstants.INDEX_TYPE_FILE_STATUS.equals(indexMsgType);
            }
            this.parsedFields = true;
            return;
        }
        if ("false".equalsIgnoreCase(event.getHeaders().get(AttributeConstants.MESSAGE_IS_ACK))) {
            this.needAck = false;
        }
        if (event instanceof SinkRspEvent) {
            SinkRspEvent rspEvent = (SinkRspEvent) event;
            this.needRspEvent = true;
            this.channel = rspEvent.getChannel();
            this.msgType = rspEvent.getMsgType();
        }
        this.parsedFields = true;
    }

    public boolean isInValidTask() {
        if (needRspEvent) {
            // check channel status
            if (channel == null || !channel.isWritable()) {
                String origAttr = event.getHeaders().getOrDefault(ConfigConstants.DECODER_ATTRS, "");
                if (logCounter.shouldPrint()) {
                    logger.warn("Prepare send msg but channel full, attr={}, channel={}", origAttr, channel);
                }
                return true;
            }
        }
        return false;
    }

    public void clear() {
        this.event = null;
        this.message = null;
        this.channel = null;
        this.groupId = null;
        this.parsedFields = false;
    }

    public boolean isParsedFields() {
        return parsedFields;
    }

    public boolean isBringEvent() {
        return bringEvent;
    }

    public String getMsgSeqId() {
        return msgSeqId;
    }

    public Message getMessage() {
        return message;
    }

    public long getPkgTime() {
        return pkgTime;
    }

    public int getMsgCnt() {
        return msgCnt;
    }

    public String getDataProxyIp() {
        return dataProxyIp;
    }

    public String getClientIp() {
        return clientIp;
    }

    /**
     * get required properties to MQ
     *
     * @param message  message object
     * @return the set time
     */
    public long setPropsToMQ(Message message) {
        long dataTimeL = Long.parseLong(event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));
        message.putSystemHeader(getStreamId(), DateTimeUtils.ms2yyyyMMddHHmm(dataTimeL));
        message.setAttrKeyVal(AttributeConstants.RCV_TIME, event.getHeaders().get(AttributeConstants.RCV_TIME));
        message.setAttrKeyVal(ConfigConstants.MSG_ENCODE_VER, event.getHeaders().get(ConfigConstants.MSG_ENCODE_VER));
        message.setAttrKeyVal(EventConstants.HEADER_KEY_VERSION,
                event.getHeaders().get(EventConstants.HEADER_KEY_VERSION));
        message.setAttrKeyVal(ConfigConstants.REMOTE_IP_KEY, this.getClientIp());
        message.setAttrKeyVal(ConfigConstants.DATAPROXY_IP_KEY,
                event.getHeaders().get(ConfigConstants.DATAPROXY_IP_KEY));
        dataTimeL = System.currentTimeMillis();
        message.setAttrKeyVal(ConfigConstants.MSG_SEND_TIME, String.valueOf(dataTimeL));
        return dataTimeL;
    }

    public long updateSendTime() {
        long curTime = System.currentTimeMillis();
        message.setAttrKeyVal(ConfigConstants.MSG_SEND_TIME, String.valueOf(curTime));
        return curTime;
    }

    /**
     * get groupId
     * 
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * get streamId
     * 
     * @return the streamId
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * get uniqId
     *
     * @return the uniqId
     */
    public String getUniqId() {
        return uniqId;
    }

    /**
     * get dt
     * 
     * @return the dt
     */
    public long getDt() {
        return dt;
    }

    /**
     * get msg size
     *
     * @return the msg size
     */
    public long getMsgSize() {
        return msgSize;
    }

    public Event getEvent() {
        return event;
    }

    /**
     * get event headers
     *
     * @return the event headers
     */
    public Map<String, String> getProperties() {
        return event.getHeaders();
    }

    public byte[] getEventBody() {
        return event.getBody();
    }

    public boolean isValidIndexMsg() {
        return isValidIndexMsg;
    }

    public boolean isStatusIndex() {
        return isStatusIndex;
    }

    public String getSrcTopic() {
        return srcTopic;
    }

    /**
     * ack
     */
    public void ack() {
        if (this.isIndex || !this.needRspEvent) {
            return;
        }
        responseV0Msg(DataProxyErrCode.SUCCESS, "");
    }

    /**
     * fail
     */
    public void fail(DataProxyErrCode errCode, String errMsg) {
        if (!needRspEvent) {
            return;
        }
        responseV0Msg(errCode, errMsg);
    }

    /**
     * isResend
     * @return whether resend message
     */
    public boolean isResend(boolean enableResend, int maxRetries) {
        return !needRspEvent
                && enableResend
                && (maxRetries < 0 || ++retries <= maxRetries);
    }

    /**
     * get required properties sent to MQ
     *
     * @param sendTime  send time
     * @return the properties
     */
    public Map<String, String> getPropsToMQ(long sendTime) {
        Map<String, String> result = new HashMap<>();
        result.put(AttributeConstants.RCV_TIME, event.getHeaders().get(AttributeConstants.RCV_TIME));
        result.put(ConfigConstants.MSG_SEND_TIME, String.valueOf(sendTime));
        result.put(ConfigConstants.MSG_ENCODE_VER, event.getHeaders().get(ConfigConstants.MSG_ENCODE_VER));
        result.put(EventConstants.HEADER_KEY_VERSION, event.getHeaders().get(EventConstants.HEADER_KEY_VERSION));
        result.put(ConfigConstants.REMOTE_IP_KEY, this.clientIp);
        result.put(ConfigConstants.DATAPROXY_IP_KEY, this.dataProxyIp);
        return result;
    }

    /**
     *  Return response to client in source
     */
    private void responseV0Msg(DataProxyErrCode errCode, String errMsg) {
        // whether index message
        if (this.isIndex) {
            return;
        }
        // check channel status
        if (channel == null || !channel.isWritable()) {
            if (logCounter.shouldPrint()) {
                logger.warn("Prepare send msg but channel full, msgType={}, attr={}, channel={}",
                        msgType, event.getHeaders(), channel);
            }
            return;
        }
        if (!this.needAck) {
            if (logger.isDebugEnabled()) {
                logger.debug("not need to rsp message: groupId = {}, streamId = {}", groupId, streamId);
            }
            return;
        }
        // build return attribute string
        if (errCode != DataProxyErrCode.SUCCESS) {
            try {
                channel.disconnect();
                channel.close();
            } catch (Throwable e) {
                //
            }
            return;
        }
        try {
            // build and send response message
            ByteBuf retData;
            String origAttr = event.getHeaders().getOrDefault(ConfigConstants.DECODER_ATTRS, "");
            if (MsgType.MSG_BIN_MULTI_BODY.equals(msgType)) {
                retData = ServerMessageHandler.buildBinMsgRspPackage(origAttr, Long.parseLong(uniqId));
            } else {
                retData = ServerMessageHandler.buildTxtMsgRspPackage(msgType, origAttr);
            }
            if (channel == null || !channel.isWritable()) {
                // release allocated ByteBuf
                retData.release();
                if (logCounter.shouldPrint()) {
                    logger.warn("Send msg but channel full, attr={}, channel={}", event.getHeaders(), channel);
                }
                return;
            }
            channel.writeAndFlush(retData);
        } catch (Throwable e) {
            //
            if (logCounter.shouldPrint()) {
                logger.warn("Send msg but failure, attr={}", event.getHeaders(), e);
            }
        }
    }
}
