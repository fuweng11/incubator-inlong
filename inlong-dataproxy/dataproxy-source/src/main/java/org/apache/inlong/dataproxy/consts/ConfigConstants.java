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

public class ConfigConstants {

    public static final String COMPONENT_NAME = "Inlong-DataProxy";

    public static final String CONFIG_PORT = "port";

    public static final String CONFIG_HOST = "host";

    public static final String MSG_FACTORY_NAME = "msg-factory-name";

    public static final String SERVICE_PROCESSOR_NAME = "service-decoder-name";

    public static final String MESSAGE_HANDLER_NAME = "message-handler-name";

    public static final String MAX_MSG_LENGTH = "max-msg-length";

    public static final String MSG_COMPRESSED = "msg-compressed";

    public static final String TOPIC = "topic";

    public static final String FILTER_EMPTY_MSG = "filter-empty-msg";

    public static final String TCP_NO_DELAY = "tcpNoDelay";

    public static final String KEEP_ALIVE = "keepAlive";

    public static final String HIGH_WATER_MARK = "highWaterMark";

    public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";
    public static final String ENABLE_EXCEPTION_RETURN = "enableExceptionReturn";

    public static final String SEND_BUFFER_SIZE = "sendBufferSize";

    public static final String TRAFFIC_CLASS = "trafficClass";

    public static final String MASTER_SERVER_URL_LIST = "master-server-url-list";
    public static final String CLIENT_STATS_OUTPUT_INTVL_SEC = "client-stats-output-intvl-sec";
    public static final long VAL_DEF_CLIENT_STATS_OUTPUT_INTVL_SEC = 300L;
    public static final long VAL_MIN_CLIENT_STATS_OUTPUT_INTVL_SEC = 0L;

    public static final String CLIENT_CONNECT_TIMEOUT_MS = "client-connect-timeout-ms";
    public static final int VAL_DEF_CONNECT_TIMEOUT_MS = 30000;
    public static final int VAL_MIN_CONNECT_TIMEOUT_MS = 1;
    public static final String CLIENT_REQUEST_TIMEOUT_MS = "client-request-timeout-ms";
    public static final int VAL_DEF_REQUEST_TIMEOUT_MS = 20000;
    public static final int VAL_MIN_REQUEST_TIMEOUT_MS = 1;

    public static final String MAX_SEND_FAILURE_WAIT_DUR_MS = "max-send-failure-wait-dur-ms";
    public static final long VAL_DEF_SEND_FAILURE_WAIT_DUR_MS = 100L;
    public static final long VAL_MIN_SEND_FAILURE_WAIT_DUR_MS = 0L;

    public static final String MAX_INFLIGHT_BUFFER_QUEUE_SIZE_KB = "max-inflight-buffer-size-KB";
    public static final int VAL_MIN_INFLIGHT_BUFFER_QUEUE_SIZE_KB = 1;

    public static final String MAX_THREADS = "max-threads";
    public static final int VAL_DEF_SINK_THREADS = 10;
    public static final int VAL_MIN_SINK_THREADS = 1;
    public static String ENABLE_MSG_CACHE_DEDUP = "enableMsgCacheDedup";
    public static final boolean VAL_DEF_ENABLE_MSG_CACHE_DEDUP = true;
    public static String MAX_CACHE_CONCURRENT_ACCESS = "max-cache-concurrent-access";
    public static final int VAL_DEF_CACHE_CONCURRENT_ACCESS = 32;
    public static final int VAL_MIN_CACHE_CONCURRENT_ACCESS = 1;
    public static String MAX_CACHE_SURVIVED_TIME_SEC = "max-cache-survived-time";
    public static final long VAL_DEF_CACHE_SURVIVED_TIME_SEC = 30;
    public static final long VAL_MIN_CACHE_SURVIVED_TIME_SEC = 1;
    public static String MAX_CACHE_SURVIVED_SIZE = "max-cache-survived-size";
    public static final int VAL_DEF_CACHE_SURVIVED_SIZE = 5000000;
    public static final int VAL_MIN_CACHE_SURVIVED_SIZE = 1;

    public static String INDEX_MSG_TYPE = "index_msg_type";
    public static String INDEX_TYPE_FILE_STATUS = "filestatus";
    public static String INDEX_TYPE_MEASURE = "measure";

    public static final String HEART_INTERVAL_SEC = "heart-interval-sec";

    public static final String PACKAGE_TIMEOUT_SEC = "package-timeout-sec";

    public static final String HEART_SERVERS = "heart-servers";

    public static final String CUSTOM_CHANNEL_PROCESSOR = "custom-cp";

    public static final String UDP_PROTOCOL = "udp";
    public static final String TCP_PROTOCOL = "tcp";

    public static final String TOPIC_KEY = "topic";
    public static final String REMOTE_IP_KEY = "srcIp";
    public static final String DATAPROXY_IP_KEY = "dpIp";
    public static final String MSG_ENCODE_VER = "msgEnType";
    public static final String MSG_SEND_TIME = "st";
    public static final String REMOTE_IDC_KEY = "idc";
    public static final String MSG_COUNTER_KEY = "msgcnt";
    public static final String PKG_COUNTER_KEY = "pkgcnt";
    public static final String PKG_TIME_KEY = "msg.pkg.time";
    public static final String TRANSFER_KEY = "transfer";
    public static final String DEST_IP_KEY = "dstIp";
    public static final String INTERFACE_KEY = "interface";
    public static final String SINK_MIN_METRIC_KEY = "sink-min-metric-topic";
    public static final String SINK_HOUR_METRIC_KEY = "sink-hour-metric-topic";
    public static final String SINK_TEN_METRIC_KEY = "sink-ten-metric-topic";
    public static final String SINK_QUA_METRIC_KEY = "sink-qua-metric-topic";
    public static final String L5_MIN_METRIC_KEY = "l5-min-metric-topic";
    public static final String L5_MIN_FAIL_METRIC_KEY = "l5-min-fail-metric-key";
    public static final String L5_HOUR_METRIC_KEY = "l5-hour-metric-topic";
    public static final String L5_ID_KEY = "l5id";
    public static final String SET_KEY = "set";
    public static final String CLUSTER_ID_KEY = "clusterId";

    public static final String DECODER_BODY = "body";
    public static final String DECODER_TOPICKEY = "topic_key";
    public static final String DECODER_ATTRS = "attrs";
    public static final String MSG_TYPE = "msg_type";
    public static final String COMPRESS_TYPE = "compress_type";
    public static final String EXTRA_ATTR = "extra_attr";
    public static final String COMMON_ATTR_MAP = "common_attr_map";
    public static final String MSG_LIST = "msg_list";
    public static final String VERSION_TYPE = "version";
    public static final String FILE_CHECK_DATA = "file-check-data";
    public static final String MINUTE_CHECK_DATA = "minute-check-data";
    public static final String SLA_METRIC_DATA = "sla-metric-data";
    public static final String SLA_METRIC_GROUPID = "manager_sla_metric";

    public static final String FILE_BODY = "file-body";
    public static final int MSG_MAX_LENGTH_BYTES = 20 * 1024 * 1024;

    public static final String SEQUENCE_ID = "sequencial_id";

    public static final String TOTAL_LEN = "totalLen";

    public static final String LINK_MAX_ALLOWED_DELAYED_MSG_COUNT = "link-max-allowed-delayed-msg-count";
    public static final long VAL_DEF_ALLOWED_DELAYED_MSG_COUNT = 80000L;
    public static final long VAL_MIN_ALLOWED_DELAYED_MSG_COUNT = 0L;

    public static final String SESSION_WARN_DELAYED_MSG_COUNT = "session-warn-delayed-msg-count";
    public static final long VAL_DEF_SESSION_WARN_DELAYED_MSG_COUNT = 2000000L;
    public static final long VAL_MIN_SESSION_WARN_DELAYED_MSG_COUNT = 0L;

    public static final String SESSION_MAX_ALLOWED_DELAYED_MSG_COUNT = "session-max-allowed-delayed-msg-count";
    public static final long VAL_DEF_SESSION_DELAYED_MSG_COUNT = 4000000L;
    public static final long VAL_MIN_SESSION_DELAYED_MSG_COUNT = 0L;
    public static final String NETTY_WRITE_BUFFER_HIGH_WATER_MARK = "netty-write-buffer-high-water-mark";
    public static final long VAL_DEF_NETTY_WRITE_HIGH_WATER_MARK = 15 * 1024 * 1024L;
    public static final long VAL_MIN_NETTY_WRITE_HIGH_WATER_MARK = 0L;

    public static final String RECOVER_THREAD_COUNT = "recover_thread_count";

    public static final String MANAGER_PATH = "/inlong/manager/openapi";
    public static final String MANAGER_GET_ALL_CONFIG_PATH = "/dataproxy/getAllConfig";
    public static final String MANAGER_HEARTBEAT_REPORT = "/heartbeat/report";

    public static final String MANAGER_AUTH_SECRET_ID = "manager.auth.secretId";
    public static final String MANAGER_AUTH_SECRET_KEY = "manager.auth.secretKey";
    // Pulsar config
    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";

    public static final String KEY_SERVICE_URL = "serviceUrl";
    public static final String KEY_AUTHENTICATION = "authentication";
    public static final String KEY_STATS_INTERVAL_SECONDS = "statsIntervalSeconds";

    public static final String KEY_ENABLEBATCHING = "enableBatching";
    public static final String KEY_BATCHINGMAXBYTES = "batchingMaxBytes";
    public static final String KEY_BATCHINGMAXMESSAGES = "batchingMaxMessages";
    public static final String KEY_BATCHINGMAXPUBLISHDELAY = "batchingMaxPublishDelay";
    public static final String KEY_MAXPENDINGMESSAGES = "maxPendingMessages";
    public static final String KEY_MAXPENDINGMESSAGESACROSSPARTITIONS = "maxPendingMessagesAcrossPartitions";
    public static final String KEY_SENDTIMEOUT = "sendTimeout";
    public static final String KEY_COMPRESSIONTYPE = "compressionType";
    public static final String KEY_BLOCKIFQUEUEFULL = "blockIfQueueFull";
    public static final String KEY_ROUNDROBINROUTERBATCHINGPARTITIONSWITCHFREQUENCY = "roundRobinRouter"
            + "BatchingPartitionSwitchFrequency";

    public static final String KEY_IOTHREADS = "ioThreads";
    public static final String KEY_MEMORYLIMIT = "memoryLimit";
    public static final String KEY_CONNECTIONSPERBROKER = "connectionsPerBroker";

    public static final String KEY_LOAD_NETWORK = "loadMonNetworkName";
    public static final String KEY_LOAD_COLLECT_INTERVALMS = "loadMonCollectIntervalMs";
    public static final String KEY_LOAD_MAX_ACC_PRINT = "loadMonMaxAccPrintCnt";

    public static final String INTER_MANAGER_SECURE_AUTH = "manager.internal.secure.auth";
    public static final String DEFAULT_INTER_MANAGER_SECURE_AUTH = "secure-authentication";

    public static final String INTER_MANAGER_NAME = "manager.internal.name";
    public static final String DEFAULT_INTER_MANAGER_NAME = "inlong_manager";

    public static final String INTER_NAMANGER_USER_NAME = "manager.internal.user.name";
    public static final String DEFAULT_INTER_NAMANGER_USER_NAME = "";

    public static final String INTER_NAMANGER_USER_KEY = "manager.internal.user.key";
    public static final String DEFAULT_INTER_NAMANGER_USER_KEY = "";

}
