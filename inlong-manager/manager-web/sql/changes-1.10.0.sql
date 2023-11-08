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

-- This is the SQL change file from version 1.9.0 to the current version 1.10.0.
-- When upgrading to version 1.10.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

ALTER TABLE `inlong_stream`
    ADD COLUMN `wrap_type` varchar(256) DEFAULT 'INLONG_MSG_V0' COMMENT 'The message body wrap type, including: RAW, INLONG_MSG_V0, INLONG_MSG_V1, etc';

UPDATE inlong_group SET status = 130 where status = 150;

-- ----------------------------
-- Table structure for stream_sink_ext
-- ----------------------------
CREATE TABLE IF NOT EXISTS `stream_sink_ext`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `sink_id`          int(11)      NOT NULL COMMENT 'Sink id',
    `key_name`         varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`        text COMMENT 'The value of the configuration item',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    INDEX `sink_id_index` (`sink_id`),
    INDEX `sink_group_stream_index` (`inlong_group_id`, `inlong_stream_id`)
);



