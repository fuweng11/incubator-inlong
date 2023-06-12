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

package org.apache.inlong.manager.common.consts;

/**
 * Global constant for the Inlong inner system.
 */
public class TencentConstants {

    public static final int HIVE_TYPE = 0;
    public static final int THIVE_TYPE = 1;

    // partition closing strategy: data De duplication verification passed
    public static final String PART_DISTINCT_VERIFIED = "DATA_DISTINCT_VERIFIED";

    // Partition closing policy: data arrival
    public static final String PART_ARRIVED = "ARRIVED";

    // Partition closing strategy: agent data volume verification passed
    public static final String PART_COUNT_VERIFIED = "AGENT_COUNT_VERIFIED";

    public static final String STRATEGY_BALANVE = "BALANCE";

    public static final String STRATEGY_RANDOM = "RANDOM";

    public static final String TUBEMQ_ROOT_DEFAULT = "/inlong_tube";

    public static final String PULSAR_ROOT_DEFAULT = "/inlong_pulsar";

    public static final String KAFKA_ROOT_DEFAULT = "/inlong_kafka";

    public static final String DATA_TYPE_CSV = "CSV";
    public static final String DATA_TYPE_RAW_CSV = "RAW_CSV";
    public static final String DATA_TYPE_KV = "KV";
    public static final String DATA_TYPE_RAW_KV = "RAW_KV";
    public static final String DATA_TYPE_INLONG_MSG_V1 = "INLONG_MSG_V1";
    public static final String DATA_TYPE_INLONG_MSG_V1_KV = "INLONG_MSG_V1_KV";
    public static final String DATA_TYPE_BINLOG = "BINLOG";
    public static final String DATA_TYPE_SEA_CUBE = "SEA_CUBE";

    public static final String FILE_FORMAT_ORC = "OrcFile";

    public static final String FILE_FORMAT_SEQUENCE = "SequenceFile";

    public static final String FILE_FORMAT_MY_SEQUENCE = "MySequenceFile";

    public static final String FILE_FORMAT_PARQUET = "Parquet";

    /**
     * Responsible person for sort consumption, consistent with the old system
     */
    public static final String SORT_CONSUME_IN_CHARGES = "t_DATA_TDBank_OP";

    /**
     * The name of the sort consumer group for the tube
     * Naming rules: clusterTag_topicName_consumer_group
     */
    public static final String SORT_TUBE_GROUP = "%s_%s_%s_consumer_group";

    public static final String OLD_SORT_TUBE_GROUP = "%s_%s_consumer_group";

    /**
     * Name of pulsar's sort consumer group
     * Naming rules: sortTaskName_groupMqResource_streamMqResource_sinkId_consumer_group
     */
    public static final String SORT_PULSAR_GROUP = "%s_%s_%s_%s_consumer_group";

    /**
     * Historical version: the name of pulsar's sort consumption group, excluding bid
     * Naming rules: sortTaskName_sinkId_consumer_group
     */
    public static final String OLD_SORT_PULSAR_GROUP = "%s_%s_consumer_group";

}
