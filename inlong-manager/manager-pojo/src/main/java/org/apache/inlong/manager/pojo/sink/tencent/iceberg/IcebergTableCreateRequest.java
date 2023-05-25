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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Request of create Iceberg table
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class IcebergTableCreateRequest {

    private String db;
    private String table;
    private String description;
    private Map<String, Object> properties;
    private List<FieldsBean> fields;
    private List<PartitionsBean> partitions;

    @Data
    public static class PropertiesBean {

        @JsonProperty("table.snapshot.retain.number")
        private int tableSnapshotRetainNumber; // FIXME check this code
        @JsonProperty("table.max-file-in-bytes")
        private long tableMaxfileinbytes; // FIXME check this code
    }

    @Data
    public static class FieldsBean {

        private String name;
        private String type;
        private String desc;
        private Map<String, String> parameters;

    }

    @Data
    public static class PartitionsBean {

        private String field;
        private String transform;
        private Map<String, String> parameter;

    }

}
