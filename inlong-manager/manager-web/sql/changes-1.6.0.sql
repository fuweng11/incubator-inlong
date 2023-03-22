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

-- This is the SQL change file from version 1.5.0 to the current version 1.6.0.
-- When upgrading to version 1.6.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;


ALTER TABLE `inlong_stream_field`
    MODIFY COLUMN `meta_field_name` varchar(120) DEFAULT NULL COMMENT 'Meta field name';

ALTER TABLE `inlong_stream_field`
    MODIFY COLUMN `field_format` text DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.';

ALTER TABLE `stream_source_field`
    MODIFY COLUMN `meta_field_name` varchar(120) DEFAULT NULL COMMENT 'Meta field name';

ALTER TABLE `stream_source_field`
    MODIFY COLUMN `field_format` text DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.';

ALTER TABLE `stream_transform_field`
    MODIFY COLUMN `meta_field_name` varchar(120) DEFAULT NULL COMMENT 'Meta field name';

ALTER TABLE `stream_transform_field`
    MODIFY COLUMN `field_format` text DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.';

ALTER TABLE `stream_sink_field`
    MODIFY COLUMN `meta_field_name` varchar(120) DEFAULT NULL COMMENT 'Meta field name';

ALTER TABLE `stream_sink_field`
    MODIFY COLUMN `field_format` text DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.';

ALTER TABLE `stream_sink_field`
    MODIFY COLUMN `source_field_name` varchar(120) DEFAULT NULL COMMENT 'Source field name';

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
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8 COMMENT ='Audit base item table';

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
       ('audit_id_sort_inner_hive_input', 'INNER_HIVE', 0, '7'),
       ('audit_id_sort_inner_hive_output', 'INNER_HIVE', 1, '8'),
       ('audit_id_sort_inner_thive_input', 'INNER_THIVE', 0, '7'),
       ('audit_id_sort_inner_thive_output', 'INNER_THIVE', 1, '8'),
       ('audit_id_sort_inner_clickhouse_input', 'INNER_CK', 0, '9'),
       ('audit_id_sort_inner_clickhouse_output', 'INNER_CK', 1, '10'),
       ('audit_id_sort_inner_iceberg_input', 'INNER_ICEBERG', 0, '17'),
       ('audit_id_sort_inner_iceberg_output', 'INNER_ICEBERG', 1, '18');

ALTER  TABLE dbsync_heartbeat CHANGE server_id server_name varchar(64)  NOT NULL DEFAULT '' COMMENT 'ServerName of the task, is the ID of data_node table';
ALTER  TABLE dbsync_heartbeat CHANGE backup_url backup_url varchar(256) DEFAULT NULL COMMENT 'URL of the standby DB server';
ALTER  TABLE dbsync_heartbeat CHANGE agent_status agent_status varchar(256) DEFAULT NULL COMMENT 'Agent running status, NORMAL, STOPPED, SWITCHED...';
ALTER  TABLE dbsync_heartbeat CHANGE db_dump_index db_dump_index bigint(20) DEFAULT NULL COMMENT 'BinLog index currently collected';
ALTER  TABLE dbsync_heartbeat CHANGE dump_position dump_position text DEFAULT NULL COMMENT 'ServerName of the task, is the ID of data_node table';

