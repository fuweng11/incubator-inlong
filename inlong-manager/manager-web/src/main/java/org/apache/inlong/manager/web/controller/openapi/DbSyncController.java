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

package org.apache.inlong.manager.web.controller.openapi;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeatRequest;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncInitInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskFullInfo;
import org.apache.inlong.common.pojo.agent.dbsync.InitTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.RunningTaskRequest;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.source.dbsync.AddFieldsRequest;
import org.apache.inlong.manager.service.core.dbsync.DbSyncAgentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * DbSync agent controller.
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-DbSync-API")
public class DbSyncController {

    @Autowired
    private DbSyncAgentService dbSyncAgentService;

    @PostMapping("/dbsync/heartbeat")
    @ApiOperation(value = "DbSync heartbeat report for indicator display")
    public Response<Boolean> heartbeat(@RequestBody DbSyncHeartbeatRequest request) {
        return Response.success(dbSyncAgentService.heartbeat(request));
    }

    @PostMapping("/dbsync/getHeartbeat")
    @ApiOperation(value = "Get the latest heartbeats By on task ID and server ID")
    public Response<DbSyncHeartbeat> getHeartbeat(@RequestParam Integer id, @RequestParam Integer dataNodeId) {
        return Response.success(dbSyncAgentService.getHeartbeat(id, dataNodeId));
    }

    @PostMapping("/dbsync/getInitInfo")
    @ApiOperation(value = "(1) Pull all server ids, includes the server version for the cluster")
    public Response<DbSyncInitInfo> getServerList(@RequestBody InitTaskRequest request) {
        return Response.success(dbSyncAgentService.getInitInfo(request));
    }

    @PostMapping("/dbsync/getRunningTask")
    @ApiOperation(value = "(2) Pull the un-deleted tasks by the IP and ServerId")
    public Response<DbSyncTaskFullInfo> getRunningTask(@RequestBody RunningTaskRequest request) {
        return Response.success(dbSyncAgentService.getRunningTask(request));
    }

    @PostMapping("/dbsync/reportAndGetTask")
    @ApiOperation(value = "(3) Report the operate result, and pull the newly tasks")
    public Response<DbSyncTaskFullInfo> reportAndGetTask(@RequestBody ReportTaskRequest request) {
        return Response.success(dbSyncAgentService.reportAndGetTask(request));
    }

    @PostMapping("/dbsync/addFields")
    @ApiOperation(value = "Report the newly fields")
    public Response<Boolean> reportNewFields(@Validated @RequestBody AddFieldsRequest request) {
        return Response.success(dbSyncAgentService.addFields(request));
    }

}
