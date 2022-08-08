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
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tencent.sc.ScConfig;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.resource.sc.ScAuthenticate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.Optional;

/**
 * security center API request service
 */
@Slf4j
@Service
public class ScApiRequestService {

    private final ScAuthenticate scAuthenticate;
    private final ScConfig scConfig;

    @Autowired
    private RestTemplate restTemplate;

    public ScApiRequestService(ScAuthenticate scAuthenticate, ScConfig scConfig) {
        this.scAuthenticate = scAuthenticate;
        this.scConfig = scConfig;
    }

    protected <T> T postCall(String api, Object params, ParameterizedTypeReference<Response<T>> typeReference) {
        Response<T> response = HttpUtils.postRequest(restTemplate, getUrl(api), params, getHeader(), typeReference);
        return checkAndGetResponseBody(response);
    }

    protected <T> T getCall(String api, Map<String, Object> params,
            ParameterizedTypeReference<Response<T>> typeReference) {
        Response<T> response = HttpUtils.getRequest(restTemplate, getUrl(api), params, getHeader(), typeReference);
        return checkAndGetResponseBody(response);
    }

    private <T> T checkAndGetResponseBody(Response<T> response) {
        if (response == null) {
            throw new BusinessException("security center interface request failed");
        }

        String errMsg = Optional.ofNullable(response).map(Response::getErrMsg).orElse("");
        Preconditions.checkTrue(response.isSuccess(), "security center request failed: " + errMsg);
        return response.getData();
    }

    private HttpHeaders getHeader() {
        HttpHeaders header = new HttpHeaders();
        header.add(ScAuthenticate.SECURE_AUTHENTICATION,
                scAuthenticate.urlEncode(scAuthenticate.tauth(scConfig.getService())));
        return header;
    }

    private String getUrl(String api) {
        return scConfig.getHost() + api;
    }

}
