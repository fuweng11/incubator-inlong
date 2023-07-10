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

package org.apache.inlong.common.pojo.agent.dbsync;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * DB server info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("DB server info")
public class DBServerInfo {

    @ApiModelProperty("Primary key")
    private Integer id;

    @ApiModelProperty("DB type, such as MySQL, Oracle, etc.")
    private String dbType;

    @ApiModelProperty("URL of DB server")
    private String url;

    @ApiModelProperty("Backup URL of DB server")
    private String backupUrl;

    @ApiModelProperty("Username")
    @ToString.Exclude
    private String username;

    @ApiModelProperty("Password")
    @ToString.Exclude
    private String password;

}
