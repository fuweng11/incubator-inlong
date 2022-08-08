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

package org.apache.inlong.manager.pojo.tencent.sc;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * INNER_HIVE resources
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("INNER_HIVE resources")
public class ScHiveResource extends ScResource {

    /**
     * community hive
     */
    public static final String TYPE_HIVE = "HIVE";
    /**
     * THIVE
     */
    public static final String TYPE_THIVE = "THIVE";

    @ApiModelProperty("database name")
    private String database;

    @ApiModelProperty("table name")
    private String table;


}
