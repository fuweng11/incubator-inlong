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
import org.apache.inlong.common.util.NetworkUtils;
import org.apache.inlong.dataproxy.base.SinkRspEvent;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.sink.mq.SimplePackProfile;
import org.apache.inlong.dataproxy.source.ServerMessageHandler;
import org.apache.inlong.sdk.commons.protocol.EventConstants;

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

    private final Event event;
    private int retries = 0;
    // cache information
    private final String groupId;
    private final String streamId;
    private final String uniqId;
    private final long dt;
    private long msgSize = 0;
    private boolean needRspEvent = false;
    private Channel channel;
    private MsgType msgType;

    public EventProfile(Event event) {
        this.event = event;
        this.retries = 0;
        this.groupId = event.getHeaders().get(AttributeConstants.GROUP_ID);
        this.streamId = event.getHeaders().get(AttributeConstants.STREAM_ID);
        this.uniqId = event.getHeaders().getOrDefault(AttributeConstants.UNIQ_ID, "");
        this.dt = NumberUtils.toLong(event.getHeaders().get(AttributeConstants.DATA_TIME));
        this.msgSize = event.getBody().length;
        if (event instanceof SinkRspEvent) {
            SinkRspEvent rspEvent = (SinkRspEvent) event;
            this.needRspEvent = true;
            this.channel = rspEvent.getChannel();
            this.msgType = rspEvent.getMsgType();
        }
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
    /**
     * ack
     */
    public void ack() {
        if (!this.needRspEvent) {
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
        result.put(ConfigConstants.REMOTE_IP_KEY, event.getHeaders().get(ConfigConstants.REMOTE_IP_KEY));
        result.put(ConfigConstants.DATAPROXY_IP_KEY, NetworkUtils.getLocalIp());
        return result;
    }

    /**
     *  Return response to client in source
     */
    private void responseV0Msg(DataProxyErrCode errCode, String errMsg) {
        // check channel status
        if (channel == null || !channel.isWritable()) {
            if (logCounter.shouldPrint()) {
                logger.warn("Prepare send msg but channel full, msgType={}, attr={}, channel={}",
                        msgType, event.getHeaders(), channel);
            }
            return;
        }
        if ("false".equals(event.getHeaders().get(AttributeConstants.MESSAGE_IS_ACK))) {
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
