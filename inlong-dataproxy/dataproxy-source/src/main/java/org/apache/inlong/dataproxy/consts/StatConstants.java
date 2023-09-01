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

package org.apache.inlong.dataproxy.consts;

public class StatConstants {

    public static final java.lang.String EVENT_SERVICE_CLOSED = "service.closed";
    public static final java.lang.String EVENT_SERVICE_SINK_UNREADY = "service.sink.unready";
    // visit
    public static final java.lang.String EVENT_VISIT_ILLEGAL = "visit.illegal";
    public static final java.lang.String EVENT_VISIT_OVERMAX = "visit.overmax";
    public static final java.lang.String EVENT_VISIT_LINKIN = "visit.linkin";
    public static final java.lang.String EVENT_VISIT_LINKOUT = "visit.linkout";
    // link
    public static final java.lang.String EVENT_LINK_READ_TIMEOUT = "link.read.timeout";
    public static final java.lang.String EVENT_LINK_FRAME_OVERMAX = "link.frame.overmax";
    public static final java.lang.String EVENT_LINK_FRAME_CORRPUTED = "link.frame.corrupted";
    public static final java.lang.String EVENT_LINK_IO_EXCEPTION = "link.io.exception";
    public static final java.lang.String EVENT_LINK_UNKNOWN_EXCEPTION = "link.unknown.exception";
    public static final java.lang.String EVENT_LINK_UNWRITABLE = "link.unwritable";
    // http link
    public static final java.lang.String EVENT_HTTP_LINK_READ_TIMEOUT = "http.link.read.timeout";
    public static final java.lang.String EVENT_HTTP_LINK_FRAME_OVERMAX = "http.link.frame.overmax";
    public static final java.lang.String EVENT_HTTP_LINK_FRAME_CORRPUTED = "http.link.frame.corrupted";
    public static final java.lang.String EVENT_HTTP_LINK_IO_EXCEPTION = "http.link.io.exception";
    public static final java.lang.String EVENT_HTTP_LINK_UNKNOWN_EXCEPTION = "http.link.unknown.exception";
    public static final java.lang.String EVENT_HTTP_LINK_UNWRITABLE = "http.link.unwritable";
    // configure
    public static final java.lang.String EVENT_CONFIG_TOPIC_MISSING = "config.topic.missing";
    public static final java.lang.String EVENT_CONFIG_IDNUM_EMPTY = "config.idnum.empty";
    public static final java.lang.String EVENT_CONFIG_GROUPIDNUM_MISSING = "config.groupidnum.missing";
    public static final java.lang.String EVENT_CONFIG_GROUP_IDNUM_INCONSTANT = "config.group.idnum.incons";
    public static final java.lang.String EVENT_CONFIG_STREAMIDNUM_MISSING = "config.streamidnum.missing";
    public static final java.lang.String EVENT_CONFIG_STREAM_IDNUM_INCONSTANT = "config.stream.idnum.incons";
    // message
    public static final java.lang.String EVENT_MSG_READABLE_EMPTY = "msg.readable.empty";
    public static final java.lang.String EVENT_MSG_READABLE_OVERMAX = "msg.readable.overmax";
    public static final java.lang.String EVENT_MSG_READABLE_UNFILLED = "msg.readable.unfilled";
    public static final java.lang.String EVENT_MSG_MSGTYPE_V0_INVALID = "msg.msgtype.v0.invalid";
    public static final java.lang.String EVENT_MSG_MSGTYPE_V1_INVALID = "msg.msgtype.v1.invalid";
    public static final java.lang.String EVENT_MSG_MSGTYPE_TDBANK_INVALID = "msg.msgtype.tdbank.invalid";
    public static final java.lang.String EVENT_MSG_BIN_TOTALLEN_BELOWMIN = "msg.bin.totallen.belowmin";
    public static final java.lang.String EVENT_MSG_TXT_TOTALLEN_BELOWMIN = "msg.txt.totallen.belowmin";
    public static final java.lang.String EVENT_MSG_DECODE_FAIL = "msg.decode.failure";
    public static final java.lang.String EVENT_MSG_METHOD_INVALID = "msg.method.invalid";
    public static final java.lang.String EVENT_MSG_PATH_INVALID = "msg.path.invalid";
    public static final java.lang.String EVENT_MSG_CONTYPE_INVALID = "msg.content.invalid";
    public static final java.lang.String EVENT_MSG_GROUPID_MISSING = "msg.groupid.missing";
    public static final java.lang.String EVENT_MSG_STREAMID_MISSING = "msg.streamid.missing";
    public static final java.lang.String EVENT_MSG_BODY_MISSING = "msg.body.missing";
    public static final java.lang.String EVENT_MSG_BODY_BLANK = "msg.body.blank";
    public static final java.lang.String EVENT_MSG_BODY_ZERO = "msg.body.zero";
    public static final java.lang.String EVENT_MSG_BODY_NEGATIVE = "msg.body.negative";
    public static final java.lang.String EVENT_MSG_BODY_UNPRESS_EXP = "msg.body.unpress.exp";
    public static final java.lang.String EVENT_MSG_BODY_OVERMAX = "msg.body.overmax";
    public static final java.lang.String EVENT_MSG_BODY_TRIP = "msg.body.trip";
    public static final java.lang.String EVENT_MSG_ATTR_NEGATIVE = "msg.attr.negative";
    public static final java.lang.String EVENT_MSG_MAGIC_UNEQUAL = "msg.magic.unequal";
    public static final java.lang.String EVENT_MSG_HB_TOTALLEN_BELOWMIN = "msg.hb.totallen.belowmin";
    public static final java.lang.String EVENT_MSG_HB_MAGIC_UNEQUAL = "msg.hb.magic.unequal";
    public static final java.lang.String EVENT_MSG_HB_LEN_MALFORMED = "msg.hb.len.malformed";
    public static final java.lang.String EVENT_MSG_BIN_LEN_MALFORMED = "msg.bin.len.malformed";
    public static final java.lang.String EVENT_MSG_TXT_LEN_MALFORMED = "msg.txt.len.malformed";
    public static final java.lang.String EVENT_MSG_ITEM_LEN_MALFORMED = "msg.item.len.malformed";
    public static final java.lang.String EVENT_MSG_ATTR_INVALID = "msg.attr.invalid";
    public static final java.lang.String EVENT_MSG_ORDER_ACK_INVALID = "msg.attr.order.noack";
    public static final java.lang.String EVENT_MSG_PROXY_ACK_INVALID = "msg.attr.proxy.noack";
    public static final java.lang.String EVENT_MSG_INDEXMSG_ILLEGAL = "msg.index.illegal";
    public static final java.lang.String EVENT_MSG_PKGTIME_ILLEGAL = "msg.pkg.time.illegal";
    public static final java.lang.String EVENT_MSG_GROUPIDNUM_ZERO = "msg.groupidnum.zero";
    public static final java.lang.String EVENT_MSG_STREAMIDNUM_ZERO = "msg.streamidnum.zero";
    public static final java.lang.String EVENT_MSG_HB_SUCCESS = "msg.hb.success";
    public static final java.lang.String EVENT_MSG_V0_POST_SUCCESS = "msg.post.v0.success";
    public static final java.lang.String EVENT_MSG_V0_POST_FAILURE = "msg.post.v0.failure";
    public static final java.lang.String EVENT_MSG_V1_POST_SUCCESS = "msg.post.v1.success";
    public static final java.lang.String EVENT_MSG_V1_POST_DROPPED = "msg.post.v1.dropped";
    public static final java.lang.String EVENT_MSG_INDEX_POST_SUCCESS = "msg.index.post.success";
    public static final java.lang.String EVENT_MSG_INDEX_POST_FAILURE = "msg.index.post.failure";
    public static final java.lang.String EVENT_MSG_XFE_PULSAR_SUCCESS = "msg.post.pulsar.xfe.success";
    public static final java.lang.String EVENT_MSG_XFE_PULSAR_DROPPED = "msg.post.pulsar.xfe.dropped";
    // sink
    public static final java.lang.String EVENT_SINK_EVENT_V1_MALFORMED = "sink.event.v1.malformed";
    public static final java.lang.String EVENT_SINK_EVENT_TAKE_SUCCESS = "sink.event.take.success";
    public static final java.lang.String EVENT_SINK_EVENT_TAKE_FAILURE = "sink.event.take.failure";
    public static final java.lang.String EVENT_SINK_EVENT_V1_FILE_TAKE_SUCCESS = "sink.event.v1.file.take.success";
    public static final java.lang.String EVENT_SINK_EVENT_V0_FILE_TAKE_SUCCESS = "sink.event.v0.file.take.success";
    public static final java.lang.String EVENT_SINK_CONFIG_TOPIC_MISSING = "sink.topic.missing";
    public static final java.lang.String EVENT_SINK_DEFAULT_TOPIC_MISSING = "default.topic.empty";
    public static final java.lang.String EVENT_SINK_DEFAULT_TOPIC_USED = "default.topic.used";
    public static final java.lang.String EVENT_SINK_PRODUCER_NULL = "sink.producer.null";
    public static final java.lang.String EVENT_SINK_CLUSTER_EMPTY = "sink.cluster.empty";
    public static final java.lang.String EVENT_SINK_CLUSTER_UNMATCHED = "sink.cluster.unmatched";
    public static final java.lang.String EVENT_SINK_CPRODUCER_NULL = "sink.cluster.producer.null";
    public static final java.lang.String EVENT_SINK_MESSAGE_DUPLICATE = "sink.msg.dup.dropped";

    // controller sink process
    public static final java.lang.String SINK_INDEX_EVENT_TAKE_SUCCESS = "sink.index.event.take.success";
    public static final java.lang.String SINK_INDEX_EVENT_TAKE_FAILURE = "sink.index.event.take.failure";
    public static final java.lang.String SINK_INDEX_FILE_TAKE_SUCCESS = "sink.index.file.take.success";
    public static final java.lang.String SINK_INDEX_ILLEGAL_DROPPED = "sink.index.illegal.dropped";
    public static final java.lang.String SINK_INDEX_DUPLICATE_DROOPED = "sink.index.duplicate.dropped";
    // status index message
    public static final java.lang.String SINK_STATUS_INDEX_OVERMAX_DROOPED = "sink.status.index.overmax.dropped";
    public static final java.lang.String SINK_STATUS_INDEX_SEND_SUCCESS = "sink.status.index.send.success";
    public static final java.lang.String SINK_STATUS_INDEX_SEND_EXCEPTION = "sink.status.index.send.exception";
    // measure index message
    public static final java.lang.String SINK_MEASURE_INDEX_OUTPUT_SUCCESS = "sink.measure.index.output.success";
    public static final java.lang.String SINK_MEASURE_INDEX_SEND_EXCEPTION = "sink.measure.index.send.exception";
    public static final java.lang.String SINK_MEASURE_INDEX_REMOTE_SUCCESS = "sink.measure.index.remote.success";
    public static final java.lang.String SINK_MEASURE_INDEX_REMOTE_FAILURE = "sink.measure.index.remote.failure";
    public static final java.lang.String SINK_MEASURE_INDEX_REMOTE_EXCEPTION = "sink.measure.index.remote.exception";

    public static final java.lang.String EVENT_SINK_SEND_EXCEPTION = "sink.send.exception";
    public static final java.lang.String EVENT_SINK_PULSAR_CLIENT_INITIAL_FAILURE =
            "sink.pulsar.client.initial.failure";
    public static final java.lang.String EVENT_SINK_PULSAR_PRODUCER_BUILD_FAILURE =
            "sink.pulsar.producer.build.failure";

    public static final java.lang.String EVENT_SINK_FAILRETRY = "sink.retry";
    public static final java.lang.String EVENT_SINK_FAILDROPPED = "sink.dropped";
    public static final java.lang.String EVENT_SINK_SUCCESS = "sink.success";
    public static final java.lang.String EVENT_SINK_FAILURE = "sink.failure";
    public static final java.lang.String EVENT_SINK_RECEIVEEXCEPT = "sink.rcvexcept";

}
