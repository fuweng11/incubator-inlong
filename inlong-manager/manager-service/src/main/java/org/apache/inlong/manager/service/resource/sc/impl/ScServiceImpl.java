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

package org.apache.inlong.manager.service.resource.sc.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tencent.sc.AppGroup;
import org.apache.inlong.manager.pojo.tencent.sc.Product;
import org.apache.inlong.manager.pojo.tencent.sc.ScPage;
import org.apache.inlong.manager.pojo.tencent.sc.Staff;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.resource.sc.ScService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * implementation of security center product interface service layer
 */
@Service
public class ScServiceImpl implements ScService {

    // private static final String LIST_USER_BY_NAME_API = "/openapi/sc/authz/staff/findStaff";
    private static final String LIST_USER_BY_NAME_API = "/api/authz/common/listStaffs";

    // private static final String GET_DETAIL_PRODUCT_API = "/openapi/sc/authz/product/";
    // private static final String LIST_PRODUCT_BY_USER_API = "/openapi/sc/authz/product/listByUser";
    private static final String GET_DETAIL_PRODUCT_API = "/api/authz/product/detail/";
    private static final String LIST_ALL_PRODUCT_API = "/api/authz/product/listAllProducts";

    // private static final String GET_DETAIL_GROUP_API = "/openapi/sc/authz/group/";
    // private static final String LIST_USER_GROUP_API = "/openapi/sc/authz/group/list/";
    private static final String GET_DETAIL_GROUP_API = "/api/authz/group/detail";
    private static final String LIST_ALL_GROUP_API = "/api/authz/group/listAllGroupsPage";

    private final ScApiRequestService scApiRequestService;
    private final ImmutableMap<String, Integer> clusterIdentifier2Id;

    {
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        // Tongle
        builder.put("tl", 1);
        // TenPay
        builder.put("cft", 2);
        // Shatin
        builder.put("hk", 5);
        // Payment bill Library
        builder.put("wxpay", 7);
        // TenPay wechat
        builder.put("cftwx", 8);
        // Pay TenPay
        builder.put("paycft", 9);
        // Hong Kong Wallet
        builder.put("wallet", 10);

        clusterIdentifier2Id = builder.build();
    }

    @Autowired
    public ScServiceImpl(ScApiRequestService scApiRequestService) {
        this.scApiRequestService = scApiRequestService;
    }

    @Override
    public List<Staff> listStaff(String username) {
        Preconditions.checkNotEmpty(username, "username cannot be null");

        /* // OpenAPI
        Map<String, Object> params = Maps.newHashMap();
        params.put("pageNum", 1);
        params.put("pageSize", 20);
        params.put("enName", username);
        ScPage<Staff> staffPage = scApiRequestService
                .getCall(LIST_USER_BY_NAME_API, params, new ParameterizedTypeReference<Response<ScPage<Staff>>>() {
                });

        return staffPage.getData();*/

        Map<String, Object> params = Maps.newHashMap();
        params.put("pageNum", 1);
        params.put("pageSize", 20);
        params.put("name", username);
        List<Staff> list = scApiRequestService
                .getCall(LIST_USER_BY_NAME_API, params, new ParameterizedTypeReference<Response<List<Staff>>>() {
                });

        list = list.size() > 20 ? list.subList(0, 20) : list;
        return list;
    }

    @Override
    public Integer getClusterIdByIdentifier(String identifier) {
        return clusterIdentifier2Id.get(identifier);
    }

    @Override
    public Product getProduct(Integer id) {
        return scApiRequestService.getCall(GET_DETAIL_PRODUCT_API + id, null,
                new ParameterizedTypeReference<Response<Product>>() {
                }
        );
    }

    @Override
    public List<Product> listProduct(String userName, String productName) {
        /* // Only OpenAPI can query the product list of the current user
        Map<String, Object> params = new HashMap<>();
        params.put("userName", userName);

        return scApiRequestService.getCall(LIST_PRODUCT_BY_USER_API, params,
                new ParameterizedTypeReference<Response<List<Product>>>() {
                });*/
        Map<String, Object> params = new HashMap<>();
        params.put("name", productName);
        params.put("pageNum", 1);
        params.put("pageSize", 20);

        ScPage<Product> result = scApiRequestService.getCall(LIST_ALL_PRODUCT_API, params,
                new ParameterizedTypeReference<Response<ScPage<Product>>>() {
                });

        return result.getData();
    }

    @Override
    public AppGroup getAppGroup(Integer id) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("id", id);
        return scApiRequestService.getCall(GET_DETAIL_GROUP_API, param,
                new ParameterizedTypeReference<Response<AppGroup>>() {
                });
    }

    @Override
    public AppGroup getAppGroup(Integer clusterId, String groupName) {
        /* // OpenAPI
        Map<String, Object> param = Maps.newHashMap();
        param.put("clusterIdentifier", clusterId);
        return scApiRequestService.getCall(GET_GROUP_DETAIL_API + groupName, param,
                new ParameterizedTypeReference<Response<AppGroup>>() {
                });
         */
        Map<String, Object> param = Maps.newHashMap();
        param.put("clusterId", clusterId);
        param.put("name", groupName);
        return scApiRequestService.getCall(GET_DETAIL_GROUP_API, param,
                new ParameterizedTypeReference<Response<AppGroup>>() {
                });
    }

    @Override
    public List<String> listAppGroupByUser(Integer productId, String userName) {
        /* // OpenAPI
        Map<String, Object> param = null;
        if (productId != null) {
            param = Maps.newHashMap();
            param.put("productId", productId);
        }
        return scApiRequestService.getCall(LIST_USER_GROUP_API + userName, param,
                new ParameterizedTypeReference<Response<List<String>>>() {
                });
         */
        /*
        Map<String, Object> params = JsonUtils.mapper.convertValue(query, Map.class);
        ScPage<AppGroup> result = scApiRequestService.getCall(LIST_ALL_API, params,
                new ParameterizedTypeReference<Response<ScPage<AppGroup>>>() {
                });
        return new PageResult<AppGroup>()
                .setList(result.getData())
                .setPageNum(query.getPageNum())
                .setPageSize(query.getPageSize())
                .setTotalPages(result.getTotalPages())
                .setTotalSize(result.getTotalCount());
         */
        Map<String, Object> params = null;
        if (productId != null) {
            params = Maps.newHashMap();
            params.put("productId", productId);
            params.put("userName", userName);
            params.put("pageNum", 1);
            params.put("pageSize", 100);
        }
        ScPage<AppGroup> resultPage = scApiRequestService.getCall(LIST_ALL_GROUP_API, params,
                new ParameterizedTypeReference<Response<ScPage<AppGroup>>>() {
                });

        List<AppGroup> groupList = resultPage.getData();
        List<String> result = new ArrayList<>(groupList.size());
        for (AppGroup appGroup : groupList) {
            result.add(appGroup.getName());
        }
        return result;
    }

    @Override
    public List<AppGroup> listAllAppGroup(String name) {
        Map<String, Object> params = Maps.newHashMap();
        params.put("pageNum", 1);
        params.put("pageSize", 20);
        params.put("groupPattern", name);

        ScPage<AppGroup> result = scApiRequestService.getCall(LIST_ALL_GROUP_API, params,
                new ParameterizedTypeReference<Response<ScPage<AppGroup>>>() {
                });
        return result.getData();
    }

}
