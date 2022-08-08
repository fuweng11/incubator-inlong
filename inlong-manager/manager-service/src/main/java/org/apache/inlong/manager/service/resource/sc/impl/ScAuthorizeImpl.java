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

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tencent.sc.ResourceGrantRequest;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.service.resource.sc.ScAuthorize;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

/**
 * security center authorization service
 */
@Slf4j
@Service
public class ScAuthorizeImpl implements ScAuthorize {

    private static final String GRANT_API = "/api/authz/authz/grantAuthz";
    // private static final String GRANT_API = "/openapi/sc/authz/resourceauth/grant";

    @Autowired
    private ScApiRequestService scApiRequestService;

    @Override
    public void grant(ResourceGrantRequest request) {
        log.debug("sc grant request:{}", JsonUtils.toJsonString(request));
        scApiRequestService.postCall(GRANT_API, request, new ParameterizedTypeReference<Response<String>>() {
        });
    }

}
