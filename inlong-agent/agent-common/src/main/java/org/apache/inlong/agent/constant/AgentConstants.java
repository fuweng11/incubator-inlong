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

package org.apache.inlong.agent.constant;

import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.inlong.agent.utils.AgentUtils;

/**
 * Configuration constants of agent.
 */
public class AgentConstants {

    public static final String AGENT_HOME = "agent.home";
    public static final String DEFAULT_AGENT_HOME = System.getProperty("agent.home");

    public static final String AGENT_LOCAL_CACHE = "agent.local.cache";
    public static final String DEFAULT_AGENT_LOCAL_CACHE = ".local";

    public static final String AGENT_LOCAL_CACHE_TIMEOUT = "agent.local.cache.timeout";
    /**
     * cache timeout in minutes.
     **/
    public static final int DEFAULT_AGENT_LOCAL_CACHE_TIMEOUT = 30;

    public static final String AGENT_LOCAL_STORE_PATH = "agent.localStore.path";
    public static final String DEFAULT_AGENT_LOCAL_STORE_PATH = ".bdb";

    public static final String AGENT_ROCKS_DB_PATH = "agent.rocks.db.path";
    public static final String DEFAULT_AGENT_ROCKS_DB_PATH = ".rocksdb";

    public static final String AGENT_UNIQ_ID = "agent.uniq.id";
    public static final String AGENT_DB_INSTANCE_NAME = "agent.db.instance.name";
    public static final String DEFAULT_AGENT_DB_INSTANCE_NAME = "agent";
    public static final String AGENT_DB_CLASSNAME = "agent.db.classname";
    public static final String DEFAULT_AGENT_DB_CLASSNAME = "org.apache.inlong.agent.db.RocksDbImp";
    // default is empty.
    public static final String AGENT_FETCHER_CLASSNAME = "agent.fetcher.classname";
    public static final String AGENT_MESSAGE_FILTER_CLASSNAME = "agent.message.filter.classname";
    public static final String AGENT_CONF_PARENT = "agent.conf.parent";
    public static final String DEFAULT_AGENT_CONF_PARENT = "conf";
    public static final String AGENT_HTTP_PORT = "agent.http.port";
    public static final int DEFAULT_AGENT_HTTP_PORT = 8008;
    public static final String AGENT_ENABLE_HTTP = "agent.http.enable";
    public static final boolean DEFAULT_AGENT_ENABLE_HTTP = false;
    public static final String TRIGGER_FETCH_INTERVAL = "trigger.fetch.interval";
    public static final int DEFAULT_TRIGGER_FETCH_INTERVAL = 1;
    public static final String TRIGGER_MAX_RUNNING_NUM = "trigger.max.running.num";
    public static final int DEFAULT_TRIGGER_MAX_RUNNING_NUM = 4096;
    public static final String AGENT_FETCH_CENTER_INTERVAL_SECONDS = "agent.fetchCenter.interval";
    public static final int DEFAULT_AGENT_FETCH_CENTER_INTERVAL_SECONDS = 5;
    public static final String AGENT_TRIGGER_CHECK_INTERVAL_SECONDS = "agent.trigger.check.interval";
    public static final int DEFAULT_AGENT_TRIGGER_CHECK_INTERVAL_SECONDS = 1;
    public static final String THREAD_POOL_AWAIT_TIME = "thread.pool.await.time";
    // time in ms
    public static final long DEFAULT_THREAD_POOL_AWAIT_TIME = 300;
    public static final String JOB_MONITOR_INTERVAL = "job.monitor.interval";
    public static final int DEFAULT_JOB_MONITOR_INTERVAL = 5;
    public static final String JOB_FINISH_CHECK_INTERVAL = "job.finish.checkInterval";
    public static final long DEFAULT_JOB_FINISH_CHECK_INTERVAL = 6L;
    public static final String TASK_RETRY_MAX_CAPACITY = "task.retry.maxCapacity";
    public static final int DEFAULT_TASK_RETRY_MAX_CAPACITY = 10000;
    public static final String TASK_MONITOR_INTERVAL = "task.monitor.interval";
    public static final int DEFAULT_TASK_MONITOR_INTERVAL = 6;
    public static final String TASK_RETRY_SUBMIT_WAIT_SECONDS = "task.retry.submit.waitSeconds";
    public static final int DEFAULT_TASK_RETRY_SUBMIT_WAIT_SECONDS = 5;
    public static final String TASK_MAX_RETRY_TIME = "task.maxRetry.time";
    public static final int DEFAULT_TASK_MAX_RETRY_TIME = 3;
    public static final String TASK_PUSH_MAX_SECOND = "task.push.maxSecond";
    public static final int DEFAULT_TASK_PUSH_MAX_SECOND = 2;
    public static final String TASK_PULL_MAX_SECOND = "task.pull.maxSecond";
    public static final int DEFAULT_TASK_PULL_MAX_SECOND = 2;
    public static final String CHANNEL_MEMORY_CAPACITY = "channel.memory.capacity";
    public static final int DEFAULT_CHANNEL_MEMORY_CAPACITY = 2000;
    public static final String TRIGGER_CHECK_INTERVAL = "trigger.check.interval";
    public static final int DEFAULT_TRIGGER_CHECK_INTERVAL = 2;
    public static final String JOB_DB_CACHE_TIME = "job.db.cache.time";
    // cache for 3 days.
    public static final long DEFAULT_JOB_DB_CACHE_TIME = 3 * 24 * 60 * 60 * 1000;
    public static final String JOB_DB_CACHE_CHECK_INTERVAL = "job.db.cache.check.interval";
    public static final int DEFAULT_JOB_DB_CACHE_CHECK_INTERVAL = 60 * 60;
    public static final String JOB_NUMBER_LIMIT = "job.number.limit";
    public static final int DEFAULT_JOB_NUMBER_LIMIT = 15;
    public static final String AGENT_LOCAL_IP = "agent.local.ip";
    public static final String DEFAULT_LOCAL_IP = "127.0.0.1";
    public static final String DEFAULT_LOCAL_HOST = "localhost";
    // default use local ip as uniq id for agent.
    public static final String DEFAULT_AGENT_UNIQ_ID = AgentUtils.getLocalIp();
    public static final String CUSTOM_FIXED_IP = "agent.custom.fixed.ip";

    public static final String AGENT_CLUSTER_NAME = "agent.cluster.name";
    public static final String AGENT_CLUSTER_TAG = "agent.cluster.tag";
    public static final String AGENT_CLUSTER_IN_CHARGES = "agent.cluster.inCharges";

    public static final String AGENT_LOCAL_UUID = "agent.local.uuid";
    public static final String AGENT_LOCAL_UUID_OPEN = "agent.local.uuid.open";
    public static final Boolean DEFAULT_AGENT_LOCAL_UUID_OPEN = false;
    public static final String AGENT_NODE_GROUP = "agent.node.group";

    public static final String PROMETHEUS_EXPORTER_PORT = "agent.prometheus.exporter.port";
    public static final int DEFAULT_PROMETHEUS_EXPORTER_PORT = 8080;

    public static final String AUDIT_ENABLE = "audit.enable";
    public static final boolean DEFAULT_AUDIT_ENABLE = true;

    public static final String AUDIT_KEY_PROXYS = "audit.proxys";
    public static final String DEFAULT_AUDIT_PROXYS = "";

    public static final String AGENT_HISTORY_PATH = "agent.history.path";
    public static final String DEFAULT_AGENT_HISTORY_PATH = ".history";

    public static final String JOB_VERSION = "job.version";
    public static final Integer DEFAULT_JOB_VERSION = 1;

    public static final String AGENT_ENABLE_OOM_EXIT = "agent.enable.oom.exit";
    public static final boolean DEFAULT_ENABLE_OOM_EXIT = false;

    // dbsync config
    public static final String DBSYNC_MSG_INDEX_KEY = "pkgIndexId";

    public static final String DBSYNC_ENABLE = "agent.dbsync.enable";
    public static final boolean DEFAULT_DBSYNC_ENABLE = false;

    public static final String DBSYNC_FILED_CHANGED_REPORT_ENABLE = "agent.dbsync.filed.changed.report.enable";
    public static final boolean DEFAULT_DBSYNC_FILED_CHANGED_REPORT_ENABLE = true;

    public static final String DBSYNC_HEART_INTERVAL = "agent.dbsync.heart.interval.msec";
    public static final long DEFAULT_DBSYNC_HEART_INTERVAL = 3 * 60 * 1000L;

    public static final String DBSYNC_CONN_INTERVAL = "agent.dbsync.conn.interval.msec";
    public static final long DEFAULT_DBSYNC_CONN_INTERVAL = 1 * 60 * 1000L;

    public static final String DBSYNC_NEED_SKIP_DELETE_DATA = "agent.dbsync.is.skip.delete.data";
    public static final boolean DEFAULT_DBSYNC_NEED_SKIP_DELETE_DATA = false;

    public static final String DBSYNC_IS_DEBUG_MODE = "agent.dbsync.is.debug.mode";
    public static final boolean DEFAULT_DBSYNC_IS_DEBUG_MODE = false;

    public static final String DBSYNC_JOB_RECV_BUFFER_KB = "agent.dbsync.job.recv.buffer.kb";
    public static final int DEFAULT_DBSYNC_JOB_RECV_BUFFER_KB = 64;

    public static final String DBSYNC_ACK_THREAD_NUM = "agent.dbsync.ack.thread.num";
    public static final int DEFAULT_ACK_THREAD_NUM = 5;

    public static final String DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS = "agent.dbsync.is.read.first.if.miss";
    public static final boolean DEFAULT_DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS = false;

    public static final String DBSYNC_RELAY_LOG_WAY = "agent.dbsync.relay-log.way";
    public static final String DEFAULT_RELAY_LOG_WAY = "memory";

    public static final String DBSYNC_MAX_CON_DB_SIZE = "agent.dbsync.max.con.db.size";
    public static final int DEFAULT_DBSYNC_MAX_CON_DB_SIZE = 1;

    public static final String DBSYNC_RELAY_LOG_MEM_SZIE = "agent.dbsync.relay-log.memory.size";
    public static final int DEFAULT_RELAY_LOG_MEM_SZIE = 1024;

    public static final String DBSYNC_RELAY_LOG_FILE_SIZE = "agent.dbsync.relay-log.file.size";
    public static final int DEFAULT_DBSYNC_RELAY_LOG_FILE_SIZE = 1024 * 1024 * 1024;

    public static final String DBSYNC_RELAY_LOG_DIFF_FILE_INDEX = "agent.dbsync.relay-log.diff.file.index";
    public static final int DEFAULT_DBSYNC_RELAY_LOG_DIFF_FILE_INDEX = 3;

    public static final String DBSYNC_RELAY_LOG_ROOT = "agent.dbsync.relay-log.root";
    public static final String DEFAULT_DBSYNC_RELAY_LOG_ROOT = "./relay/";

    public static final String DBSYNC_RELAY_LOG_BLOG_SIZE = "agent.dbsync.relay-log.block.size";
    public static final int DEFAULT_DBSYNC_RELAY_LOG_BLOG_SIZE = 1024 * 1024 * 4;

    public static final String DBSYNC_MAX_COLUMN_VALUE_SIZE = "agent.dbsync.max.column.value.size";
    public static final int DEFAULT_MAX_COLUMN_VALUE_SIZE = 1024 * 1024;

    public static final String DBSYNC_MYSQL_BINLOG_FORMATS = "agent.dbsync.mysql.binlog.format";
    public static final String DEFAULT_MYSQL_BINLOG_FORMATS = "ROW,STATEMENT,MIXED";

    public static final String DBSYNC_MYSQL_BINLOG_IMAGES = "agent.dbsync.mysql.binlog.images";
    public static final String DEFAULT_MYSQL_BINLOG_IMAGES = "FULL,MINIMAL,NOBLOB";

    public static final String DBSYNC_IS_NEED_TRANSACTION = "agent.dbsync.is.need.transaction";
    public static final boolean DEFAULT_IS_NEED_TRANSACTION = true;

    public static final String DBSYNC_JOB_DO_SWITCH_CNT = "agent.dbsync.job.do.switch.cnt";
    public static final int DEFAULT_DBSYNC_JOB_DO_SWITCH_CNT = 10;

    public static final long DBSYNC_BINLOG_START_OFFEST = 4L;

    public static final String DBSYNC_SKIP_ZK_POSITION_ENABLE = "agent.dbsync.skip.zk.position.enable";
    public static final boolean DEFAULT_DBSYNC_SKIP_ZK_POSITION_ENABLE = false;

    public static final String DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS = "agent.dbsync.skip.zk.position.enable";
    public static final long DEFAULT_DBSYNC_HA_COORDINATOR_MONITOR_INTERVAL_MS = 2 * 60 * 1000L;

    public static final String DBSYNC_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD = "agent.dbsync.ha.run.mode.change."
            + "candidate.threshold";
    public static final float DEFAULT_HA_RUN_NODE_CHANGE_CANDIDATE_MAX_THRESHOLD = 0.5F;

    public static final String DBSYNC_HA_RUN_NODE_CHANGE_MAX_THRESHOLD =
            "agent.dbsync.ha.run.mode.change.max.threshold";
    public static final float DEFAULT_HA_RUN_NODE_CHANGE_MAX_THRESHOLD = 0.7F;

    public static final String DBSYNC_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD =
            "agent.dbsync.ha.loadbalance.compare.load.usage.threshold";
    public static final int DEFAULT_HA_LOADBALANCE_COMPARE_LOAD_USAGE_THRESHOLD = 10;

    public static final String DBSYNC_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD =
            "agent.dbsync.ha.loadbalance.check.load.threshold";
    public static final float DEFAULT_HA_LOADBALANCE_CHECK_LOAD_THRESHOLD = 0.6F;

    public static final String DBSYNC_HA_POSITION_UPDATE_INTERVAL_MS = "agent.dbsync.ha.position.update.intervalms";
    public static final long DEFAULT_HA_POSITION_UPDATE_INTERVAL_MS = 10 * 1000L;

    public static final String DBSYNC_HA_JOB_STATE_MONITOR_INTERVAL_MS = "agent.dbsync.ha.job.state.monitor.intervalms";
    public static final long DEFAULT_HA_JOB_STATE_MONITOR_INTERVAL_MS = 1 * 60 * 1000L;

    public static final String DBSYNC_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD =
            "agent.dbsync.ha.job.state.monitor.intervalms";
    public static final int DEFAULT_JOB_UNACK_LOGPOSITIONS_MAX_THRESHOLD = 100000;

    public static final String DBSYNC_UPDATE_POSITION_INTERVAL = "agent.dbsync.default.update.position.interval";
    public static final int DEFAULT_UPDATE_POSITION_INTERVAL = 60;

    public static final String DBSYNC_RESETTING_CHECK_INTERVAL = "agent.dbsync.resetting.check.interval";
    public static final int DEFAULT_RESETTING_CHECK_INTERVAL = 60;

    //
    public static final String DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES =
            "agent.dbsync.filed.changed.report.retry.max.times";
    public static final int DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES = 10;

    public static final String DBSYNC_FIELD_CHANGED_REPORT_INTREVALS = "agent.dbsync.resetting.check.interval";
    public static final int DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_INTREVALS = 10000;

    public static final String DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE =
            "agent.dbsync.filed.changed.max.message.queue.size";
    public static final int DEFAULT_DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE = 10000;

    public static final String DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST = "agent.dbsync.filed.changed.mysql.type.list";
    public static final String DEFAULT_DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST = "bit,tinyint,smallint"
            + ",mediumint,int,integer,bigint,dec,float,double,decimal,date,time"
            + ",datetime,timestamp,year,char,varchar,binary,varbinary,blob,text"
            + ",enum,set,geometry,point,linestring,polygon,multipoint,multilinestring"
            + ",multipolygon,geometrycollection,json";

    public static final String NULL_STRING = "NULL";

    public static final String DBSYNC_JOB_BLOCK_TIME_MSEC = "agent.dbsync.job.block.time.msec";
    public static final long DEFAULT_DBSYNC_JOB_BLOCK_TIME_MSEC = 1000 * 60 * 5L;

    public static final String AGENT_METRIC_LISTENER_CLASS = "agent.domainListeners";
    public static final String AGENT_METRIC_LISTENER_CLASS_DEFAULT =
            "org.apache.inlong.agent.metrics.AgentPrometheusMetricListener";

    // pulsar sink config
    public static final String PULSAR_CLIENT_IO_TREHAD_NUM = "agent.sink.pulsar.client.io.thread.num";
    public static final int DEFAULT_PULSAR_CLIENT_IO_TREHAD_NUM = Math.max(1,
            SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));

    public static final String PULSAR_CONNECTION_PRE_BROKER = "agent.sink.pulsar.connection.pre.broker";
    public static final int DEFAULT_PULSAR_CONNECTION_PRE_BROKER = 1;

    public static final String PULSAR_CLIENT_TIMEOUT_SECOND = "agent.sink.pulsar.send.timeout.second";
    public static final int DEFAULT_PULSAR_CLIENT_TIMEOUT_SECOND = 30;

    public static final String PULSAR_CLIENT_ENABLE_BATCH = "agent.sink.pullsar.enable.batch";
    public static final boolean DEFAULT_PULSAR_CLIENT_ENABLE_BATCH = true;

    public static final String PULSAR_CLIENT_BLOCK_IF_QUEUE_FULL = "agent.sink.pulsar.block.if.queue.full";
    public static final boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;

    public static final String PULSAR_CLIENT_MAX_PENDING_MESSAGES = "agent.sink.pulsar.max.pending.messages";
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 10000;

    public static final String PULSAR_CLIENT_MAX_PENDING_MESSAGES_ACROSS_PARTITION =
            "agent.sink.pulsar.max.messages.across.partition";
    public static final int DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITION = 500000;

    public static final String PULSAR_CLIENT_MAX_BATCH_BYTES = "agent.sink.pulsar.max.batch.bytes";
    public static final int DEFAULT_MAX_BATCH_BYTES = 128 * 1024;

    public static final String PULSAR_CLIENT_MAX_BATCH_MESSAGES = "agent.sink.pulsar.max.batch.messages";
    public static final int DEFAULT_MAX_BATCH_MESSAGES = 1000;

    public static final String PULSAR_CLIENT_MAX_BATCH_INTERVAL_MILLIS = "agent.sink.pulsar.max.batch.interval.millis";
    public static final int DEFAULT_MAX_BATCH_INTERVAL_MILLIS = 1;

    public static final String PULSAR_CLIENT_COMPRESSION_TYPE = "agent.sink.pulsar.compression.type";
    public static final String DEFAULT_COMPRESSION_TYPE = "NONE";

    public static final String PULSAR_CLIENT_PRODUCER_NUM = "agent.sink.pulsar.producer.num";
    public static final String KAFKA_SINK_PRODUCER_NUM = "agent.sink.kafka.producer.num";
    public static final int DEFAULT_PRODUCER_NUM = 3;

    public static final String PULSAR_CLIENT_ENABLE_ASYNC_SEND = "agent.sink.pulsar.enbale.async.send";
    public static final String KAFKA_PRODUCER_ENABLE_ASYNC_SEND = "agent.sink.kafka.enbale.async.send";
    public static final boolean DEFAULT_ENABLE_ASYNC_SEND = true;

    public static final String PULSAR_SINK_SEND_QUEUE_SIZE = "agent.sink.pulsar.send.queue.size";
    public static final String KAFKA_SINK_SEND_QUEUE_SIZE = "agent.sink.kafka.send.queue.size";
    public static final int DEFAULT_SEND_QUEUE_SIZE = 20000;

    public static final String DEFAULT_KAFKA_SINK_SEND_ACKS = "1";
    public static final long DEFAULT_KAFKA_SINK_SYNC_SEND_TIMEOUT_MS = 3000;

    public static final String DEFAULT_KAFKA_SINK_SEND_COMPRESSION_TYPE = "none";

    public static final String DEFAULT_KAFKA_SINK_SEND_KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    public static final String DEFAULT_KAFKA_SINK_SEND_VALUE_SERIALIZER =
            "org.apache.kafka.common.serialization.ByteArraySerializer";

    // dbsync metric config
    public static final String DBSYNC_SEND_THREAD_SIZE = "dbsync.metric.send.thread.size";
    public static final int DEFAULT_DBSYNC_SEND_THREAD_SIZE = 2;

    public static final String DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM = "dbsync.metric.pulsar.client.io.thread.num";
    public static final int DEFAULT_DBSYNC_PULSAR_CLIENT_IO_THREAD_NUM = 10;

    public static final String DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE = "dbsync.metric.send.queue.size";
    public static final int DEFAULT_DBSYNC_SEND_QUEUE_SIZESEND_QUEUE_SIZE = 5000;

    public static final String DBSYNC_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER =
            "dbsync.metric.pulsar.client.connections.pre.broker";
    public static final int DEFAULT_METRIC_PULSAR_CLIENT_CONNECTIONS_PRE_BROKER = 2;

    public static final String DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES =
            "dbsync.metric.pulsar.status.monitor.interval.minutes";
    public static final int DEFAULT_DBSYNC_METRIC_PULSAR_STATUS_MONITOR_INTERVAL_MINUTES = 1;

    public static final String DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES = "dbsync.metric.pulsar.max.pending.messages";
    public static final int DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES = 1000;

    public static final String DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS =
            "dbsync.metric.max.pending.messages.across.partitions";
    public static final int DEFAULT_DBSYNC_METRIC_PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 500000;

    public static final String DBSYNC_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS =
            "dbsync.metric.max.batching.publish.delay.millis";
    public static final int DEFAULT_PULSAR_MAX_BATCHING_PUBLISH_DELAY_MILLIS = 1;

    public static final String DBSYNC_PULSAR_COMPRESSION_TYPE = "dbsync.metric.pulsar.compression.type";
    public static final String DEFAULT_DBSYNC_PULSAR_COMPRESSION_TYPE = "snappy";

    public static final String DBSYNC_PULSAR_MAX_BATCHING_SIZE = "dbsync.metric.pulsar.max.batch.size";
    public static final int DEFAULT_DBSYNC_PULSAR_MAX_BATCHING_SIZE = 131072;

    public static final String DBSYNC_PULSAR_ENABLE_BATCHING = "dbsync.metric.pulsar.enable.batching";
    public static final Boolean DEFAULT_DBSYNC_PULSAR_ENABLE_BATCHING = false;

    public static final String DBSYNC_PULSAR_BLOCKING_QUEUE = "dbsync.metric.pulsar.block.queue";
    public static final Boolean DEFAULT_DBSYNC_PULSAR_BLOCKING_QUEUE = true;

    public static final String PULSAR_ASYNC = "dbsync.metric.pulsar.async";
    public static final Boolean DEFAULT_PULSAR_ASYNC = true;

    public static final String PULSAR_BUSY_SIZE = "dbsync.metric.pulsar.queue.busy.size";
    public static final int DEFAULT_PULSAR_BUSY_SIZE = 10000;

    public static final String DBSYNC_PULSAR_CLUSTER = "dbsync.metric.pulsar.cluster";
    public static final String DEFAULT_DBSYNC_PULSAR_CLUSTER = "";

    public static final String DBSYNC_PULSAR_TOPIC = "dbsync.metric.pulsar.topic";
    public static final String DEFAULT_DBSYNC_PULSAR_TOPIC = "persistent://dbsync/audit/dbsync_audit_metric";
}
