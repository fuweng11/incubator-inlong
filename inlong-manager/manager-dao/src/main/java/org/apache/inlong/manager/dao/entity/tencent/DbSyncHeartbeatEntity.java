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

package org.apache.inlong.manager.dao.entity.tencent;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class DbSyncHeartbeatEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer id;
    private String instance;
    private String serverId;
    private String currentDb;
    private String url;
    private String backupUrl;

    private String agentStatus;
    private String taskIds;
    private Long dbDumpIndex;
    private String dumpPosition;
    private String maxLogPosition;
    private String errorMsg;

    private Long reportTime;
    private Date createTime;
    private Date modifyTime;

}