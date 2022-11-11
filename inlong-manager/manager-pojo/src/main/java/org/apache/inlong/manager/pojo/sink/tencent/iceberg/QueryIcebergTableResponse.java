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

package org.apache.inlong.manager.pojo.sink.tencent.iceberg;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Response of query Iceberg table
 */
@Data
public class QueryIcebergTableResponse {

    /**
     * code=0, create table success
     * code=20001, create table failed
     * code=20005, table already exists
     */
    private int code;
    private String message;
    private TableStructure data;

    @Data
    public static class TableStructure {

        private String name;
        private String database;
        private String format;
        private String location;
        private int snapshots;
        private int snapshotRetention;
        private Map<String, String> properties;
        private Object desc;
        private Object nestFields;
        private TableInfosForTdsortBean tableInfosForTdsort;
        private List<FieldsBean> fields;
        private List<String> partitions;

        @Data
        public static class TableInfosForTdsortBean {

            private String tableLocation;
            private String schemaAsJson;
            private String partitionSpecAsJson;
            private Map<String, String> hadoopConfProps;
            private Map<String, String> tableProperties;
        }

        @Data
        public static class FieldsBean {

            private String name;
            private int id;
            private String type;
            private String desc;
            private String transform;
        }
    }
}
