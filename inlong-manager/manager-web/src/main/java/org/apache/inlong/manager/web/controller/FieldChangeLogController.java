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

package org.apache.inlong.manager.web.controller;

import org.apache.inlong.manager.dao.entity.tencent.FieldChangeLogEntity;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.core.dbsync.FieldChangeLogService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Field change log controller.
 */
@RestController
@RequestMapping("/api")
@Api(tags = "FieldChangeLog-API")
public class FieldChangeLogController {

    @Autowired
    private FieldChangeLogService fieldChangeLogService;

    @RequestMapping(value = "/fieldChangeLog/list", method = RequestMethod.POST)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "inlongGroupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "inlongStreamId", dataTypeClass = String.class, required = true)
    })
    @ApiOperation(value = "List field change log by paginating")
    public Response<PageResult<FieldChangeLogEntity>> listByCondition(@RequestParam String inlongGroupId,
            @RequestParam String inlongStreamId) {
        return Response.success(fieldChangeLogService.list(inlongGroupId, inlongStreamId));
    }
}
