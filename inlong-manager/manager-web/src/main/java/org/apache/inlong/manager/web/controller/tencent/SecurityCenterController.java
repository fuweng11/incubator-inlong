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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tencent.sc.AppGroup;
import org.apache.inlong.manager.pojo.tencent.sc.Product;
import org.apache.inlong.manager.pojo.tencent.sc.ScHiveResource;
import org.apache.inlong.manager.pojo.tencent.sc.Staff;
import org.apache.inlong.manager.service.resource.sc.ScService;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.tencentauth.StaffBaseInfo;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Security center related services
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Security center interface - product and Application Group")
public class SecurityCenterController {

    @Autowired
    private ScService securityService;

    @Autowired
    private SmartGateService smartGateService;

    @GetMapping("/sc/staff/list")
    @ApiOperation(value = "Employee list - name fuzzy query")
    @ApiImplicitParam(name = "name", value = "Employee name:", dataTypeClass = String.class, required = true)
    public Response<List<Staff>> listStaff(@RequestParam String name) {
        List<Staff> staffList = securityService.listStaff(name);
        return Response.success(staffList);
    }

    @GetMapping("/sc/staff/{name}")
    @ApiOperation(value = "Employee information - by name")
    @ApiImplicitParam(name = "name", value = "Employee name:", dataTypeClass = String.class, required = true)
    public Response<StaffBaseInfo> getStaffByName(@PathVariable String name) {
        return Response.success(smartGateService.getStaffByEnName(name));
    }

    @GetMapping("/sc/product/{id}")
    @ApiOperation(value = "Product details - ID query")
    public Response<Product> getProduct(@PathVariable Integer id) {
        return Response.success(securityService.getProduct(id));
    }

    @GetMapping("/sc/product/list")
    @ApiOperation(value = "Product list - current user (only fuzzy name is supported temporarily)")
    @ApiImplicitParam(name = "productName", value = "Product Name:", dataTypeClass = String.class, required = false)
    public Response<List<Product>> listProduct(@RequestParam(required = false) String productName) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(securityService.listProduct(username, productName));
    }

    @ApiOperation(value = "Application group details - ID query")
    @GetMapping("/sc/appgroup/{id}")
    public Response<AppGroup> getAppGroup(@PathVariable Integer id) {
        return Response.success(securityService.getAppGroup(id));
    }

    @GetMapping("/sc/appgroup/detail")
    @ApiOperation(value = "Application group details - name query")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "clusterId", value = "Cluster ID", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "name", value = "App group name", dataTypeClass = String.class, required = true)
    })
    public Response<AppGroup> getAppGroup(@RequestParam Integer clusterId, @RequestParam String name) {
        return Response.success(securityService.getAppGroup(clusterId, name));
    }

    @GetMapping("/sc/appgroup/my")
    @ApiOperation(value = "Application group list - list under current user and specified product ID")
    @ApiImplicitParam(name = "productId", value = "Product ID", dataTypeClass = Integer.class, required = true)
    public Response<List<String>> listMyAppGroup(@RequestParam Integer productId) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(securityService.listAppGroupByUser(productId, currentUser));
    }

    @ApiOperation(value = "Application group list - list of all application groups")
    @GetMapping("/sc/appgroup/list")
    @ApiImplicitParam(name = "name", value = "Application group name", dataTypeClass = String.class, required = false)
    public Response<List<AppGroup>> listAppGroup(@RequestParam(required = false) String name) {
        return Response.success(securityService.listAllAppGroup(name));
    }

    @ApiOperation(value = "Database list - list of all database by appGroup and clusterTag")
    @GetMapping("/sc/database/list")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "Application group name", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "clusterTag", value = "Cluster tag", dataTypeClass = String.class, required = true)
    })
    public Response<List<ScHiveResource>> listDatabase(@RequestParam String name, @RequestParam String clusterTag) {
        return Response.success(securityService.listDatabase(name, clusterTag));
    }

}
