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

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for inlong_group
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_group`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`        varchar(256) NOT NULL COMMENT 'Inlong group id, filled in by the user, undeleted ones cannot be repeated',
    `name`                   varchar(128)          DEFAULT '' COMMENT 'Inlong group name, English, Chinese, numbers, etc',
    `description`            varchar(256)          DEFAULT '' COMMENT 'Description of inlong group',
    `mq_type`                varchar(20)           DEFAULT 'TUBEMQ' COMMENT 'The message queue type, high throughput: TUBEMQ, high consistency: PULSAR',
    `mq_resource`            varchar(128) NOT NULL COMMENT 'MQ resource, for TubeMQ, its Topic, for Pulsar, its Namespace',
    `daily_records`          int(11)               DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`          int(11)               DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`           int(11)               DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`             int(11)               DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `enable_zookeeper`       tinyint(1)            DEFAULT '0' COMMENT 'Whether to enable the zookeeper, 0-disable, 1-enable',
    `enable_create_resource` tinyint(1)            DEFAULT '1' COMMENT 'Whether to enable create resource? 0-disable, 1-enable',
    `inlong_group_mode`      tinyint(1)            DEFAULT '0' COMMENT 'InLong group mode, Standard mode(include Data Ingestion and Synchronization): 0, DataSync mode(only Data Synchronization): 1',
    `data_report_type`       int(4)                DEFAULT '0' COMMENT 'Data report type. 0: report to DataProxy and respond when the DataProxy received data. 1: report to DataProxy and respond after DataProxy sends data. 2: report to MQ and respond when the MQ received data',
    `inlong_cluster_tag`     varchar(128)          DEFAULT NULL COMMENT 'The cluster tag, which links to inlong_cluster table',
    `ext_params`             mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `tenant`                 varchar(256)          DEFAULT 'public' COMMENT 'Inlong tenant of the inlong group',
    `in_charges`             varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `followers`              varchar(512)          DEFAULT NULL COMMENT 'Name of followers, separated by commas',
    `status`                 int(4)                DEFAULT '100' COMMENT 'Inlong group status',
    `previous_status`        int(4)                DEFAULT '100' COMMENT 'Previous group status',
    `is_deleted`             int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`                int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_group` (`inlong_group_id`, `is_deleted`),
    INDEX `group_status_deleted_index` (`status`, `is_deleted`),
    INDEX `group_modify_time_index` (`modify_time`),
    INDEX `group_cluster_tag_index` (`inlong_cluster_tag`)
);

-- ----------------------------
-- Table structure for inlong_group_ext
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_group_ext`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id` varchar(256) NOT NULL COMMENT 'Inlong group id',
    `key_name`        varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`       text              DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`      int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time`     timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_group_key` (`inlong_group_id`, `key_name`)
);

-- ----------------------------
-- Table structure for inlong_cluster_tag
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_cluster_tag`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_tag` varchar(128) NOT NULL COMMENT 'Cluster tag',
    `ext_params`  mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `description` varchar(256)          DEFAULT '' COMMENT 'Description of cluster tag',
    `tenant`      varchar(256)          DEFAULT 'public' COMMENT 'Inlong tenant of the inlong cluster tag',
    `in_charges`  varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`      int(4)                DEFAULT '0' COMMENT 'Cluster status',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_cluster_tag` (`cluster_tag`, `is_deleted`)
);

-- ----------------------------
-- Table structure for inlong_cluster
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_cluster`
(
    `id`                    int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`                  varchar(128) NOT NULL COMMENT 'Cluster name',
    `display_name`          varchar(128)          DEFAULT NULL COMMENT 'Cluster display name',
    `type`                  varchar(20)           DEFAULT '' COMMENT 'Cluster type, such as: TUBEMQ, PULSAR, DATAPROXY, etc',
    `url`                   varchar(512)          DEFAULT NULL COMMENT 'Cluster URL',
    `cluster_tags`          varchar(512)          DEFAULT NULL COMMENT 'Cluster tag, separated by commas',
    `ext_tag`               varchar(128)          DEFAULT NULL COMMENT 'Extension tag, for extended use',
    `token`                 varchar(512)          DEFAULT NULL COMMENT 'Cluster token',
    `ext_params`            mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `description`           varchar(256)          DEFAULT '' COMMENT 'Description of cluster',
    `heartbeat`             mediumtext            DEFAULT NULL COMMENT 'Cluster heartbeat info',
    `tenant`                varchar(256) NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of cluster',
    `in_charges`            varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`                int(4)                DEFAULT '0' COMMENT 'Cluster status',
    `is_deleted`            int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`               varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`              varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`               int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_cluster` (`name`, `type`, `is_deleted`),
    INDEX `cluster_type_index` (`type`)
);

-- ----------------------------
-- Table structure for inlong_cluster_node
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_cluster_node`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_id`     int(11)      NOT NULL COMMENT 'Id of the parent cluster',
    `type`          varchar(20)  NOT NULL COMMENT 'Cluster type, such as: AGENT, DATAPROXY, etc',
    `ip`            varchar(512) NOT NULL COMMENT 'Cluster IP, separated by commas, such as: 127.0.0.1:8080,host2:8081',
    `port`          int(6)       NULL COMMENT 'Cluster port',
    `protocol_type` varchar(20)           DEFAULT NULL COMMENT 'DATAPROXY Source listen protocol type, such as: TCP/HTTP',
    `node_load`     int(11)               DEFAULT '-1' COMMENT 'Current load value of the node',
    `ext_params`    mediumtext            DEFAULT NULL COMMENT 'Another fields will be saved as JSON string',
    `description`   varchar(256)          DEFAULT '' COMMENT 'Description of cluster node',
    `status`        int(4)                DEFAULT '0' COMMENT 'Cluster status',
    `is_deleted`    int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`       varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`      varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`       int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_cluster_node` (`parent_id`, `type`, `ip`, `port`, `protocol_type`, `is_deleted`)
);

-- ----------------------------
-- Table structure for inlong_consume
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_consume`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `consumer_group`   varchar(256) NOT NULL COMMENT 'Consumer group name, filled in by the user, undeleted ones cannot be repeated',
    `description`      varchar(256)          DEFAULT '' COMMENT 'Inlong consume description',
    `mq_type`          varchar(10)           DEFAULT 'TUBEMQ' COMMENT 'Message queue type, high throughput: TUBEMQ, high consistency: PULSAR',
    `topic`            varchar(256) NOT NULL COMMENT 'The target topic of this consume',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'The target inlong group id of this consume',
    `filter_enabled`   int(2)                DEFAULT '0' COMMENT 'Whether to filter consume, 0: not filter, 1: filter',
    `inlong_stream_id` varchar(256)          DEFAULT NULL COMMENT 'The target inlong stream id of this consume, needed if the filter_enabled=1',
    `ext_params`       mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `tenant`           varchar(256) NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of the inlong consume',
    `in_charges`       varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`           int(4)                DEFAULT '100' COMMENT 'Inlong consume status',
    `previous_status`  int(4)                DEFAULT '100' COMMENT 'Previous status',
    `is_deleted`       int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`          int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_consume` (`consumer_group`, `is_deleted`)
);


-- ----------------------------
-- Table structure for data_node
-- ----------------------------
CREATE TABLE IF NOT EXISTS `data_node`
(
    `id`                      int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`                    varchar(128) NOT NULL COMMENT 'Node name',
    `display_name`            varchar(128)          DEFAULT NULL COMMENT 'Data node display name',
    `type`                    varchar(20)           DEFAULT '' COMMENT 'Node type, such as: MYSQL, HIVE, KAFKA, ES, etc',
    `url`                     varchar(512)          DEFAULT NULL COMMENT 'Node URL',
    `username`                varchar(128)          DEFAULT NULL COMMENT 'Username for node if needed',
    `token`                   varchar(512)          DEFAULT NULL COMMENT 'Node token',
    `ext_params`              mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `description`             varchar(256)          DEFAULT '' COMMENT 'Description of data node',
    `tenant`                  varchar(256) NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of the data node',
    `in_charges`              varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`                  int(4)                DEFAULT '0' COMMENT 'Node status',
    `is_deleted`              int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                 varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`                varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`             timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`             timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`                 int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_data_node` (`name`, `type`, `is_deleted`),
    INDEX `datanode_tenant_index` (`tenant`, `is_deleted`)
);

-- ----------------------------
-- Table structure for stream_source_cmd_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_source_cmd_config`
(
    `id`                  int(11)     NOT NULL AUTO_INCREMENT COMMENT 'cmd id',
    `cmd_type`            int(11)     NOT NULL,
    `task_id`             int(11)     NOT NULL,
    `specified_data_time` varchar(64) NOT NULL,
    `bSend`               tinyint(1)  NOT NULL,
    `create_time`         timestamp   NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp   NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `result_info`         varchar(64)      DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_source_cmd_config` (`task_id`, `bSend`, `specified_data_time`)
);

-- ----------------------------
-- Table structure for inlong_stream
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_stream`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Inlong stream id, non-deleted globally unique',
    `name`             varchar(64)           DEFAULT NULL COMMENT 'The name of the inlong stream page display, can be Chinese',
    `description`      varchar(256)          DEFAULT '' COMMENT 'Description of inlong stream',
    `mq_resource`      varchar(128)          DEFAULT NULL COMMENT 'MQ resource, in one stream, corresponding to the filter ID of TubeMQ, corresponding to the topic of Pulsar',
    `data_type`        varchar(20)           DEFAULT NULL COMMENT 'Data type, including: CSV, KEY-VALUE, JSON, AVRO, etc.',
    `data_encoding`    varchar(8)            DEFAULT 'UTF-8' COMMENT 'Data encoding format, including: UTF-8, GBK, etc.',
    `data_separator`   varchar(8)            DEFAULT NULL COMMENT 'The source data field separator',
    `data_escape_char` varchar(8)            DEFAULT NULL COMMENT 'Source data field escape character, the default is NULL (NULL), stored as 1 character',
    `sync_send`        tinyint(1)            DEFAULT '0' COMMENT 'order_preserving 0: none, 1: yes',
    `daily_records`    int(11)               DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`    int(11)               DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`     int(11)               DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`       int(11)               DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `storage_period`   int(11)               DEFAULT '1' COMMENT 'The storage period of data in MQ, unit: day',
    `ext_params`       mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `status`           int(4)                DEFAULT '100' COMMENT 'Inlong stream status',
    `previous_status`  int(4)                DEFAULT '100' COMMENT 'Previous status',
    `is_deleted`       int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`          int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_stream` (`inlong_group_id`, `inlong_stream_id`, `is_deleted`)
);

-- ----------------------------
-- Table structure for inlong_stream_ext
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_stream_ext`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `key_name`         varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`        text COMMENT 'The value of the configuration item',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_stream_key` (`inlong_group_id`, `inlong_stream_id`, `key_name`)
);

-- ----------------------------
-- Table structure for inlong_stream_field
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_stream_field`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id`    varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `is_predefined_field` tinyint(1)   DEFAULT '0' COMMENT 'Whether it is a predefined field, 0: no, 1: yes',
    `field_name`          varchar(120) NOT NULL COMMENT 'field name',
    `field_value`         varchar(128) DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`      varchar(256) DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`          varchar(20)  NOT NULL COMMENT 'field type',
    `field_comment`       varchar(50)  DEFAULT NULL COMMENT 'Field description',
    `is_meta_field`       smallint(3)  DEFAULT '0' COMMENT 'Is this field a meta field? 0: no, 1: yes',
    `meta_field_name`     varchar(120) DEFAULT NULL COMMENT 'Meta field name',
    `field_format`        text         DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.',
    `rank_num`            smallint(6)  DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`          int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`),
    INDEX `stream_field_group_stream_index` (`inlong_group_id`, `inlong_stream_id`)
);

-- ----------------------------
-- Table structure for operation_log
-- ----------------------------
CREATE TABLE IF NOT EXISTS `operation_log`
(
    `id`                  int(11)   NOT NULL AUTO_INCREMENT,
    `authentication_type` varchar(64)        DEFAULT NULL COMMENT 'Authentication type',
    `operation_type`      varchar(256)       DEFAULT NULL COMMENT 'Operation type',
    `http_method`         varchar(64)        DEFAULT NULL COMMENT 'Request method',
    `invoke_method`       varchar(256)       DEFAULT NULL COMMENT 'Invoke method',
    `operator`            varchar(256)       DEFAULT NULL COMMENT 'Operator name',
    `proxy`               varchar(256)       DEFAULT NULL COMMENT 'Proxy user',
    `request_url`         varchar(256)       DEFAULT NULL COMMENT 'Request URL',
    `remote_address`      varchar(256)       DEFAULT NULL COMMENT 'Request IP',
    `cost_time`           bigint(20)         DEFAULT NULL COMMENT 'Time-consuming',
    `body`                mediumtext COMMENT 'Request body',
    `param`               mediumtext COMMENT 'Request parameters',
    `status`              int(4)             DEFAULT NULL COMMENT 'Operate status',
    `request_time`        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Request time',
    `err_msg`             mediumtext COMMENT 'Error message',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for stream_source
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_source`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`    varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `source_name`         varchar(128) NOT NULL DEFAULT '' COMMENT 'source_name',
    `source_type`         varchar(20)           DEFAULT '0' COMMENT 'Source type, including: FILE, DB, etc',
    `template_id`         int(11)               DEFAULT NULL COMMENT 'Id of the template task this agent belongs to',
    `agent_ip`            varchar(40)           DEFAULT NULL COMMENT 'Ip of the agent running the task, NULL if this is a template task',
    `uuid`                varchar(30)           DEFAULT NULL COMMENT 'Mac uuid of the agent running the task',
    `data_node_name`      varchar(128)          DEFAULT NULL COMMENT 'Node name, which links to data_node table',
    `inlong_cluster_name` varchar(128)          DEFAULT NULL COMMENT 'Cluster name of the agent running the task',
    `inlong_cluster_node_group` varchar(512)      DEFAULT NULL COMMENT 'Cluster node group',
    `serialization_type`  varchar(20)           DEFAULT NULL COMMENT 'Serialization type, support: csv, json, canal, avro, etc',
    `snapshot`            mediumtext            DEFAULT NULL COMMENT 'Snapshot of this source task',
    `report_time`         timestamp    NULL COMMENT 'Snapshot time',
    `start_position`      text                  DEFAULT NULL COMMENT 'The location where binlog starts to collect',
    `ext_params`          mediumtext            DEFAULT NULL COMMENT 'Another fields will be saved as JSON string, such as filePath, dbName, tableName, etc',
    `operate_log`         mediumtext            DEFAULT NULL COMMENT 'Background operate log',
    `version`             int(11)               DEFAULT '1' COMMENT 'Stream source version',
    `status`              int(4)                DEFAULT '110' COMMENT 'Stream source status',
    `previous_status`     int(4)                DEFAULT '110' COMMENT 'Previous status',
    `is_deleted`          int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`            varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_source_name` (`inlong_group_id`, `inlong_stream_id`, `source_name`, `is_deleted`),
    INDEX `source_status_index` (`status`, `is_deleted`),
    INDEX `source_agent_ip_index` (`agent_ip`, `is_deleted`),
    INDEX `source_template_id_index` (`template_id`)
);

-- ----------------------------
-- Table structure for stream_transform
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_transform`
(
    `id`                   int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`      varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`     varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `transform_name`       varchar(128) NOT NULL COMMENT 'Transform name, unique in one stream',
    `transform_type`       varchar(20)  NOT NULL COMMENT 'Transform type, including: splitter, filter, joiner, etc.',
    `pre_node_names`       text         NOT NULL COMMENT 'Pre node names of transform in this stream',
    `post_node_names`      text COMMENT 'Post node names of transform in this stream',
    `transform_definition` mediumtext   NOT NULL COMMENT 'Transform definition in json type',
    `version`              int(11)      NOT NULL DEFAULT '1' COMMENT 'Stream transform version',
    `is_deleted`           int(11)      NOT NULL DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`              varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`             varchar(64)           DEFAULT '' COMMENT 'Modifier name',
    `create_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_transform_name` (`inlong_group_id`, `inlong_stream_id`, `transform_name`, `is_deleted`)
);

-- ----------------------------
-- Table structure for stream_sink
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_sink`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`        varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id`       varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `sink_type`              varchar(15)           DEFAULT 'HIVE' COMMENT 'Sink type, including: HIVE, ES, etc',
    `sink_name`              varchar(128) NOT NULL DEFAULT '' COMMENT 'Sink name',
    `description`            varchar(500) NULL COMMENT 'Sink description',
    `enable_create_resource` tinyint(1)            DEFAULT '1' COMMENT 'Whether to enable create sink resource? 0-disable, 1-enable',
    `inlong_cluster_name`    varchar(128)          DEFAULT NULL COMMENT 'Cluster name, which links to inlong_cluster table',
    `data_node_name`         varchar(128)          DEFAULT NULL COMMENT 'Node name, which links to data_node table',
    `sort_task_name`         varchar(512)          DEFAULT NULL COMMENT 'Sort task name or task ID',
    `sort_consumer_group`    varchar(512)          DEFAULT NULL COMMENT 'Consumer group name for Sort task',
    `ext_params`             mediumtext   NULL COMMENT 'Another fields, will be saved as JSON type',
    `operate_log`            mediumtext            DEFAULT NULL COMMENT 'Background operate log',
    `status`                 int(4)                DEFAULT '100' COMMENT 'Stream sink status',
    `previous_status`        int(4)                DEFAULT '100' COMMENT 'Previous status',
    `is_deleted`             int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`                int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_sink_name` (`inlong_group_id`, `inlong_stream_id`, `sink_name`, `is_deleted`)
);

-- ----------------------------
-- Table structure for stream_source_field
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_source_field`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `source_id`        int(11)      NOT NULL COMMENT 'Source id',
    `source_type`      varchar(15)  NOT NULL COMMENT 'Source type',
    `field_name`       varchar(120) NOT NULL COMMENT 'field name',
    `field_value`      varchar(128) DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`   varchar(256) DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`       varchar(20)  NOT NULL COMMENT 'field type',
    `field_comment`    varchar(50)  DEFAULT NULL COMMENT 'Field description',
    `is_meta_field`    smallint(3)  DEFAULT '0' COMMENT 'Is this field a meta field? 0: no, 1: yes',
    `meta_field_name`  varchar(120) DEFAULT NULL COMMENT 'Meta field name',
    `field_format`     text         DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.',
    `rank_num`         smallint(6)  DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`       int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`),
    INDEX `source_id_index` (`source_id`),
    INDEX `source_group_stream_index` (`inlong_group_id`, `inlong_stream_id`)
);

-- ----------------------------
-- Table structure for stream_transform_field
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_transform_field`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`   varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`  varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `transform_id`      int(11)      NOT NULL COMMENT 'Transform id',
    `transform_type`    varchar(15)  NOT NULL COMMENT 'Transform type',
    `field_name`        varchar(120) NOT NULL COMMENT 'Field name',
    `field_value`       varchar(128)  DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`    varchar(256)  DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`        varchar(50)  NOT NULL COMMENT 'Field type',
    `field_comment`     varchar(2000) DEFAULT NULL COMMENT 'Field description',
    `is_meta_field`     smallint(3)   DEFAULT '0' COMMENT 'Is this field a meta field? 0: no, 1: yes',
    `meta_field_name`   varchar(120)  DEFAULT NULL COMMENT 'Meta field name',
    `field_format`      text          DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.',
    `rank_num`          smallint(6)   DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`        int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `origin_node_name`  varchar(256)  DEFAULT '' COMMENT 'Origin node name which stream field belongs',
    -- The source node name of the transport field
    `origin_field_name` varchar(50)   DEFAULT '' COMMENT 'Origin field name before transform operation',
    PRIMARY KEY (`id`),
    INDEX `transform_id_index` (`transform_id`),
    INDEX `transform_group_stream_index` (`inlong_group_id`, `inlong_stream_id`)
);

-- ----------------------------
-- Table structure for stream_sink_field
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_sink_field`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`   varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`  varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `sink_id`           int(11)      NOT NULL COMMENT 'Sink id',
    `sink_type`         varchar(15)  NOT NULL COMMENT 'Sink type',
    `source_field_name` varchar(50)   DEFAULT NULL COMMENT 'Source field name',
    `source_field_type` varchar(50)   DEFAULT NULL COMMENT 'Source field type',
    `field_name`        varchar(120) NOT NULL COMMENT 'Field name',
    `field_type`        varchar(50)  NOT NULL COMMENT 'Field type',
    `field_comment`     varchar(2000) DEFAULT NULL COMMENT 'Field description',
    `ext_params`        text COMMENT 'Field ext params',
    `is_meta_field`     smallint(3)   DEFAULT '0' COMMENT 'Is this field a meta field? 0: no, 1: yes',
    `meta_field_name`   varchar(120)  DEFAULT NULL COMMENT 'Meta field name',
    `field_format`      text          DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.',
    `origin_node_name`  varchar(256)  DEFAULT '' COMMENT 'Origin node name which stream field belongs',
    `origin_field_name` varchar(50)   DEFAULT '' COMMENT 'Origin field name before transform operation',
    `rank_num`          smallint(6)   DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`        int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`),
    INDEX `sink_id_index` (`sink_id`),
    INDEX `sink_group_stream_index` (`inlong_group_id`, `inlong_stream_id`)
);

-- ----------------------------
-- Table structure for user
-- ----------------------------
CREATE TABLE IF NOT EXISTS `user`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`            varchar(256) NOT NULL COMMENT 'Username',
    `password`        varchar(64)  NOT NULL COMMENT 'Password md5',
    `secret_key`      varchar(256)          DEFAULT NULL COMMENT 'Auth key for public network access',
    `public_key`      text                  DEFAULT NULL COMMENT 'Public key for asymmetric data encryption',
    `private_key`     text                  DEFAULT NULL COMMENT 'Private key for asymmetric data encryption',
    `encrypt_version` int(11)               DEFAULT NULL COMMENT 'Encryption key version',
    `account_type`    int(11)      NOT NULL DEFAULT '1' COMMENT 'Account type, 0-manager 1-normal',
    `due_date`        datetime              DEFAULT NULL COMMENT 'Due date for user',
    `ext_params`      text COMMENT 'Json extension info',
    `status`          int(11)               DEFAULT '100' COMMENT 'Status',
    `is_deleted`      int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`         varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`        varchar(256)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time`     datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`     datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`         int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_user_name` (`name`)
);

-- ----------------------------
-- Table structure for role
-- ----------------------------
CREATE TABLE IF NOT EXISTS `role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `role_code`   varchar(100) NOT NULL COMMENT 'Role code',
    `role_name`   varchar(256) NOT NULL COMMENT 'Role Chinese name',
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to disabled, 0: enabled, 1: disabled',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`     varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(256)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_role_code` (`role_code`),
    UNIQUE KEY `unique_role_name` (`role_name`)
);

-- ----------------------------
-- Table structure for tenant user_role
-- ----------------------------
CREATE TABLE IF NOT EXISTS `tenant_user_role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `username`   varchar(256)  NOT NULL COMMENT 'Username',
    `role_code`   varchar(256) NOT NULL COMMENT 'User role code',
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to disabled, 0: enabled, 1: disabled',
    `tenant`      varchar(256) NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`     varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(256)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_tenant_user` (`username`, `tenant`, `is_deleted`),
    INDEX `index_tenant` (`tenant`, `is_deleted`)
);

-- ----------------------------
-- Table structure for inlong_user_role
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_user_role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `username`   varchar(256)  NOT NULL COMMENT 'Username',
    `role_code`   varchar(256) NOT NULL COMMENT 'User role code',
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to disabled, 0: enabled, 1: disabled',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`     varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(256)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_user_role` (`username`, `role_code`, `is_deleted`)
);

INSERT INTO `inlong_user_role` (`username`, `role_code`, `creator`)
VALUES ('admin', 'INLONG_ADMIN', 'inlong_init');

-- ----------------------------
-- Table structure for workflow_approver
-- ----------------------------
CREATE TABLE IF NOT EXISTS `workflow_approver`
(
    `id`           int(11)       NOT NULL AUTO_INCREMENT,
    `process_name` varchar(256)  NOT NULL COMMENT 'Process name',
    `task_name`    varchar(256)  NOT NULL COMMENT 'Approval task name',
    `tenant`       varchar(256)  NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of workflow approver',
    `approvers`    varchar(1024) NOT NULL COMMENT 'Approvers, separated by commas',
    `creator`      varchar(64)   NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(64)            DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`   int(11)                DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `version`      int(11)       NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    INDEX `process_name_task_name_index` (`process_name`, `task_name`)
);

-- create workflow approver for newly inlong group and inlong consume.
INSERT INTO `workflow_approver`(`process_name`, `task_name`, `tenant`, `approvers`, `creator`, `modifier`)
VALUES ('APPLY_GROUP_PROCESS', 'ut_admin', 'public', 'admin', 'inlong_init', 'inlong_init'),
       ('APPLY_CONSUME_PROCESS', 'ut_admin', 'public', 'admin', 'inlong_init', 'inlong_init');

-- ----------------------------
-- Table structure for workflow_event_log
-- ----------------------------
CREATE TABLE IF NOT EXISTS `workflow_event_log`
(
    `id`                   int(11)      NOT NULL AUTO_INCREMENT,
    `process_id`           int(11)      NOT NULL,
    `process_name`         varchar(256)  DEFAULT NULL COMMENT 'Process name',
    `process_display_name` varchar(256) NOT NULL COMMENT 'Process name',
    `inlong_group_id`      varchar(256)  DEFAULT NULL COMMENT 'Inlong group id',
    `task_id`              int(11)       DEFAULT NULL COMMENT 'Task ID',
    `element_name`         varchar(256) NOT NULL COMMENT 'Name of the component that triggered the event',
    `element_display_name` varchar(256) NOT NULL COMMENT 'Display name of the component that triggered the event',
    `event_type`           varchar(64)  NOT NULL COMMENT 'Event type: process / task ',
    `event`                varchar(64)  NOT NULL COMMENT 'Event name',
    `listener`             varchar(1024) DEFAULT NULL COMMENT 'Event listener name',
    `status`               int(11)      NOT NULL COMMENT 'Status',
    `async`                tinyint(1)   NOT NULL COMMENT 'Asynchronous or not',
    `ip`                   varchar(64)   DEFAULT NULL COMMENT 'IP address executed by listener',
    `start_time`           datetime     NOT NULL COMMENT 'Monitor start execution time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'Listener end time',
    `remark`               text COMMENT 'Execution result remark information',
    `exception`            mediumtext COMMENT 'Exception information',
    PRIMARY KEY (`id`),
    INDEX event_group_status_index (`inlong_group_id`, `status`),
    INDEX event_process_task_index (`process_id`, `task_id`)
);

-- ----------------------------
-- Table structure for workflow_process
-- ----------------------------
CREATE TABLE IF NOT EXISTS `workflow_process`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT,
    `name`             varchar(256) NOT NULL COMMENT 'Process name',
    `display_name`     varchar(256) NOT NULL COMMENT 'Process display name',
    `type`             varchar(256)          DEFAULT NULL COMMENT 'Process classification',
    `title`            varchar(256)          DEFAULT NULL COMMENT 'Process title',
    `inlong_group_id`  varchar(256)          DEFAULT NULL COMMENT 'Inlong group id to which this process belongs',
    `inlong_stream_id` varchar(256)          DEFAULT NULL COMMENT 'Inlong stream id to which this process belongs',
    `tenant`           varchar(256) NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of workflow process',
    `applicant`        varchar(256) NOT NULL COMMENT 'Applicant',
    `status`           varchar(64)  NOT NULL COMMENT 'Status',
    `form_data`        mediumtext COMMENT 'Form information',
    `start_time`       datetime     NOT NULL COMMENT 'Start time',
    `end_time`         datetime              DEFAULT NULL COMMENT 'End time',
    `ext_params`       mediumtext   NULL COMMENT 'Another fields, will be saved as JSON type',
    `hidden`           tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to hidden, 0: not hidden, 1: hidden',
    PRIMARY KEY (`id`),
    INDEX process_group_status_index (`inlong_group_id`, `status`)
);

-- ----------------------------
-- Table structure for workflow_task
-- ----------------------------
CREATE TABLE IF NOT EXISTS `workflow_task`
(
    `id`                   int(11)       NOT NULL AUTO_INCREMENT,
    `type`                 varchar(64)   NOT NULL COMMENT 'Task type: UserTask / ServiceTask',
    `process_id`           int(11)       NOT NULL COMMENT 'Process ID',
    `process_name`         varchar(256)  NOT NULL COMMENT 'Process name',
    `process_display_name` varchar(256)  NOT NULL COMMENT 'Process name',
    `name`                 varchar(256)  NOT NULL COMMENT 'Task name',
    `display_name`         varchar(256)  NOT NULL COMMENT 'Task display name',
    `tenant`               varchar(256)  NOT NULL DEFAULT 'public' COMMENT 'Inlong tenant of workflow task',
    `applicant`            varchar(64)   DEFAULT NULL COMMENT 'Applicant',
    `approvers`            varchar(1024) NOT NULL COMMENT 'Approvers',
    `status`               varchar(64)   NOT NULL COMMENT 'Status',
    `operator`             varchar(256)  DEFAULT NULL COMMENT 'Actual operator',
    `remark`               varchar(1024) DEFAULT NULL COMMENT 'Remark information',
    `form_data`            mediumtext COMMENT 'Form information submitted by the current task',
    `start_time`           datetime      NOT NULL COMMENT 'Start time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'End time',
    `ext_params`           mediumtext COMMENT 'Extended params, will be saved as JSON string',
    PRIMARY KEY (`id`),
    INDEX process_status_index (`process_id`, `status`),
    INDEX process_name_index (`process_id`, `name`)
);

-- ----------------------------
-- Table structure for db_collector_detail_task
-- ----------------------------
CREATE TABLE IF NOT EXISTS `db_collector_detail_task`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `main_id`       varchar(128) NOT NULL COMMENT 'main task id',
    `type`          int(11)      NOT NULL COMMENT 'task type',
    `time_var`      varchar(64)  NOT NULL COMMENT 'time variable',
    `db_type`       int(11)      NOT NULL COMMENT 'db type',
    `ip`            varchar(64)  NOT NULL COMMENT 'db ip',
    `port`          int(11)      NOT NULL COMMENT 'db port',
    `db_name`       varchar(64)  NULL COMMENT 'db name',
    `user`          varchar(64)  NULL COMMENT 'user name',
    `password`      varchar(64)  NULL COMMENT 'password',
    `sql_statement` varchar(256) NULL COMMENT 'sql statement',
    `offset`        int(11)      NOT NULL COMMENT 'offset for the data source',
    `total_limit`   int(11)      NOT NULL COMMENT 'total limit in a task',
    `once_limit`    int(11)      NOT NULL COMMENT 'limit for one query',
    `time_limit`    int(11)      NOT NULL COMMENT 'time limit for task',
    `retry_times`   int(11)      NOT NULL COMMENT 'max retry times if task failes',
    `group_id`      varchar(64)  NULL COMMENT 'group id',
    `stream_id`     varchar(64)  NULL COMMENT 'stream id',
    `state`         int(11)      NOT NULL COMMENT 'task state',
    `create_time`   timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `modify_time`   timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'modify time',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for sort_source_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `sort_source_config`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'Cluster name',
    `task_name`    varchar(128) NOT NULL COMMENT 'Task name',
    `zone_name`    varchar(128) NOT NULL COMMENT 'Cache zone name',
    `topic`        varchar(128) DEFAULT '' COMMENT 'Topic',
    `ext_params`   mediumtext   DEFAULT NULL COMMENT 'Another fields, will be saved as JSON type',
    PRIMARY KEY (`id`),
    INDEX `sort_source_config_index` (`cluster_name`, `task_name`)
);

-- ----------------------------
-- Table structure for inlong component heartbeat
-- ----------------------------
CREATE TABLE IF NOT EXISTS `component_heartbeat`
(
    `id`               int(11)     NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `component`        varchar(64) NOT NULL DEFAULT '' COMMENT 'Component name, such as: Agent, Sort...',
    `instance`         varchar(64) NOT NULL DEFAULT '' COMMENT 'Component instance, can be ip, name...',
    `status_heartbeat` mediumtext           DEFAULT NULL COMMENT 'Status heartbeat info',
    `metric_heartbeat` mediumtext           DEFAULT NULL COMMENT 'Metric heartbeat info',
    `report_time`      bigint(20)  NOT NULL COMMENT 'Report time',
    `create_time`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_component_heartbeat` (`component`, `instance`)
);

-- ----------------------------
-- Table structure for inlong group heartbeat
-- ----------------------------
CREATE TABLE IF NOT EXISTS `group_heartbeat`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `component`        varchar(64)  NOT NULL DEFAULT '' COMMENT 'Component name, such as: Agent, Sort...',
    `instance`         varchar(64)  NOT NULL DEFAULT '' COMMENT 'Component instance, can be ip, name...',
    `inlong_group_id`  varchar(256) NOT NULL DEFAULT '' COMMENT 'Owning inlong group id',
    `status_heartbeat` mediumtext            DEFAULT NULL COMMENT 'Status heartbeat info',
    `metric_heartbeat` mediumtext            DEFAULT NULL COMMENT 'Metric heartbeat info',
    `report_time`      bigint(20)   NOT NULL COMMENT 'Report time',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_group_heartbeat` (`component`, `instance`, `inlong_group_id`)
);

-- ----------------------------
-- Table structure for inlong stream heartbeat
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_heartbeat`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `component`        varchar(64)  NOT NULL DEFAULT '' COMMENT 'Component name, such as: Agent, Sort...',
    `instance`         varchar(64)  NOT NULL DEFAULT '' COMMENT 'Component instance, can be ip, name...',
    `inlong_group_id`  varchar(256) NOT NULL DEFAULT '' COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL DEFAULT '' COMMENT 'Owning inlong stream id',
    `status_heartbeat` mediumtext            DEFAULT NULL COMMENT 'Status heartbeat info',
    `metric_heartbeat` mediumtext            DEFAULT NULL COMMENT 'Metric heartbeat info',
    `report_time`      bigint(20)   NOT NULL COMMENT 'Report time',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_stream_heartbeat` (`component`, `instance`, `inlong_group_id`, `inlong_stream_id`)
);

-- ----------------------------
-- Table structure for inlong_tenant
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_tenant`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `name`         varchar(256) NOT NULL COMMENT 'Namespace, not support modification',
    `description`  varchar(256) DEFAULT '' COMMENT 'Description of tenant',
    `is_deleted`   int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`      varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(256) DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`      int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_tenant_key` (`name`, `is_deleted`)
);

-- ----------------------------
-- Insert inlong_tenant item
-- ----------------------------
INSERT INTO `inlong_tenant`(`name`, `description`, `creator`, `modifier`)
VALUES ('public', 'Default tenant', 'admin', 'admin'),
       ('another', 'Another tenant', 'admin', 'admin');

-- ----------------------------
-- Table structure for audit_base
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_base`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`             varchar(256) NOT NULL COMMENT 'Audit base name',
    `type`             varchar(20)  NOT NULL COMMENT 'Audit base item type, such as: AGENT, DATAPROXY, etc',
    `is_sent`          int(4)       NOT NULL DEFAULT '0' COMMENT '0: received, 1: sent',
    `audit_id`         varchar(11)  NOT NULL COMMENT 'Audit ID mapping of audit name',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_audit_base_type` (`type`, `is_sent`),
    UNIQUE KEY `unique_audit_base_name` (`name`)
);

-- ----------------------------
-- Insert audit_base item
-- ----------------------------
INSERT INTO `audit_base`(`name`, `type`, `is_sent`, `audit_id`)
VALUES ('audit_sdk_collect', 'SDK', 0, '1'),
       ('audit_sdk_sent', 'SDK', 1, '2'),
       ('audit_agent_collect', 'AGENT', 0, '3'),
       ('audit_agent_sent', 'AGENT', 1, '4'),
       ('audit_dataproxy_received', 'DATAPROXY', 0, '5'),
       ('audit_dataproxy_sent', 'DATAPROXY', 1, '6'),
       ('audit_sort_hive_input', 'HIVE', 0, '7'),
       ('audit_sort_hive_output', 'HIVE', 1, '8'),
       ('audit_sort_clickhouse_input', 'CLICKHOUSE', 0, '9'),
       ('audit_sort_clickhouse_output', 'CLICKHOUSE', 1, '10'),
       ('audit_sort_es_input', 'ELASTICSEARCH', 0, '11'),
       ('audit_sort_es_output', 'ELASTICSEARCH', 1, '12'),
       ('audit_sort_starrocks_input', 'STARROCKS', 0, '13'),
       ('audit_sort_starrocks_output', 'STARROCKS', 1, '14'),
       ('audit_sort_hudi_input', 'HUDI', 0, '15'),
       ('audit_sort_hudi_output', 'HUDI', 1, '16'),
       ('audit_sort_iceberg_input', 'ICEBERG', 0, '17'),
       ('audit_sort_iceberg_output', 'ICEBERG', 1, '18'),
       ('audit_sort_hbase_input', 'HBASE', 0, '19'),
       ('audit_sort_hbase_output', 'HBASE', 1, '20'),
       ('audit_sort_doris_input', 'DORIS', 0, '21'),
       ('audit_sort_doris_output', 'DORIS', 1, '22'),
       ('audit_sort_mysql_input', 'MYSQL', 0, '23'),
       ('audit_sort_mysql_output', 'MYSQL', 1, '24'),
       ('audit_sort_kudu_input', 'KUDU', 0, '25'),
       ('audit_sort_kudu_output', 'KUDU', 1, '26'),
       ('audit_sort_postgres_input', 'POSTGRESQL', 0, '27'),
       ('audit_sort_postgres_output', 'POSTGRESQL', 1, '28');

-- ----------------------------
-- Table structure for dbsync heartbeat
-- ----------------------------
CREATE TABLE IF NOT EXISTS `dbsync_heartbeat`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `instance`         varchar(64)  NOT NULL DEFAULT '' COMMENT 'Component instance, can be ip, name...',
    `server_name`      varchar(64)  NOT NULL DEFAULT '' COMMENT 'ServerName of the task, is the ID of data_node table',
    `current_db`       varchar(256) NOT NULL DEFAULT '' COMMENT 'Currently collected DB',
    `url`              varchar(256) NOT NULL DEFAULT '' COMMENT 'URL of the DB server, such as 127.0.0.1:3306',
    `backup_url`       varchar(256) NOT NULL DEFAULT '' COMMENT 'URL of the standby DB server',
    `agent_status`     varchar(256) NOT NULL DEFAULT '' COMMENT 'Agent running status, NORMAL, STOPPED, SWITCHED...',
    `task_ids`         text                  DEFAULT NULL COMMENT 'Task IDs being collected by DbSync, is the ID of stream_source table',
    `db_dump_index`    bigint(20)   NOT NULL COMMENT 'BinLog index currently collected',
    `dump_position`    varchar(256) NOT NULL DEFAULT '' COMMENT 'BinLog position currently collected, will be saved as JSON string',
    `max_log_position` text                  DEFAULT NULL COMMENT 'BinLog maximum position of the current DB, will be saved as a JSON string',
    `error_msg`        text                  DEFAULT NULL COMMENT 'Error message',
    `report_time`      bigint(20)   NOT NULL COMMENT 'Report time',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_dbsync_heartbeat` (`instance`, `server_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Dbsync_heartbeat';

-- ----------------------------
-- Table structure for bg_info
-- ----------------------------

CREATE TABLE IF NOT EXISTS `bg_info`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Primary key ID - align the ID of the HR system BG',
    `name`                   varchar(256) NOT NULL COMMENT 'BG full name',
    `abbrev_name`            varchar(64)       DEFAULT '' COMMENT 'BG abbreviation: such as TEG',
    `us_bg_id`               int(11)      NOT NULL COMMENT 'BG ID mapping of us system',
    PRIMARY KEY (`id`),
    UNIQUE KEY `bg_info_name_uindex` (`name`),
    UNIQUE KEY `bg_info_abbrev_name_uindex` (`abbrev_name`)

    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT ='BG info table';

-- ----------------------------
-- Create bg info
-- ----------------------------
INSERT INTO `bg_info`(`id`, `name`, `abbrev_name`, `us_bg_id`)
VALUES (5, 'SNG社交网络事业群', 'SNG', 5),
       (7, 'ECC腾讯电商控股公司', 'ECC', 7),
       (12, 'INTERLINKENTERPRISE互联网企业', 'INTERLINKENTERPRISE', 12),
       (78, 'S1职能系统－职能线', 'S1', 9),
       (953, 'CDG企业发展事业群', 'CDG', 1),
       (954, 'MIG移动互联网事业群', 'MIG', 3),
       (956, 'IEG互动娱乐事业群', 'IEG', 2),
       (957, 'OMG网络媒体事业群', 'OMG', 4),
       (958, 'TEG技术工程事业群', 'TEG', 6),
       (2233, 'S2职能系统－财经线', 'S2', 10),
       (2234, 'S3职能系统－HR与管理线', 'S3', 11),
       (14129, 'WXG微信事业群', 'WXG', 8),
       (29292, 'PCG平台与内容事业群', 'PCG', 13),
       (29294, 'CSIG云与智慧产业事业群', 'CSIG', 14);

-- ----------------------------
-- Table structure for inlong_tenant
-- ----------------------------
CREATE TABLE IF NOT EXISTS `inlong_tenant`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `name`         varchar(256) NOT NULL COMMENT 'Namespace, not support modification',
    `description`  varchar(256) DEFAULT '' COMMENT 'Description of tenant',
    `is_deleted`   int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`      varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(256) DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`      int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_user_role_key` (`name`, `is_deleted`)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8 COMMENT ='Inlong tenant table';

-- ----------------------------
-- Insert inlong_tenant item
-- ----------------------------
INSERT INTO `inlong_tenant`(`name`, `description`, `creator`, `modifier`)
VALUES ('public', 'Default tenant', 'admin', 'admin');
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
