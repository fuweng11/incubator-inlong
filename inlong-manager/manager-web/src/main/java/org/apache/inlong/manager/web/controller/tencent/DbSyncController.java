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

package org.apache.inlong.manager.web.controller.tencent;

import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.core.dbsync.DbSyncAgentService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * DbSync agent controller.
 */
@RestController
@RequestMapping("/api")
@Api(tags = "DbSync-API")
public class DbSyncController {

    @Autowired
    private DbSyncAgentService dbSyncAgentService;

    @PostMapping("/dbsync/getHeartbeat")
    @ApiOperation(value = "Get the latest heartbeats By on task ID and server name")
    public Response<DbSyncHeartbeat> getHeartbeat(@RequestParam Integer id, @RequestParam String dataNodeName) {
        return Response.success(dbSyncAgentService.getHeartbeat(id, dataNodeName));
    }

}
