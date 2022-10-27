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

package org.apache.inlong.manager.service.tencentauth;

import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.service.tencentauth.bean.SmartGateConfig;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Tencent SmartGate API Service
 */
@Slf4j
@Service
public class SmartGateApiRequestService {

    private static final String HEADER_TIMESTAMP = "timestamp";
    private static final String HEADER_SIGNATURE = "signature";

    private final SmartGateConfig smartGateConfig;

    @Autowired
    RestTemplate template;

    @Autowired
    public SmartGateApiRequestService(SmartGateConfig smartGateConfig) {
        this.smartGateConfig = smartGateConfig;
    }

    public <T> T getCall(String api, Map<String, Object> params,
            ParameterizedTypeReference<T> typeReference) {
        return HttpUtils.getRequest(template, getUrl(api), params, generateAuthHeaders(), typeReference);
    }

    private String getUrl(String api) {
        return smartGateConfig.getHost() + "/" + api;
    }

    private HttpHeaders generateAuthHeaders() {
        String timestamp = String.valueOf((int) (System.currentTimeMillis() / 1000));
        String signature = createSignature(timestamp, smartGateConfig.getToken());
        log.debug("timestamp:" + timestamp + ", signature: " + signature);
        HttpHeaders header = new HttpHeaders();

        header.add(HEADER_TIMESTAMP, timestamp);
        header.add(HEADER_SIGNATURE, signature);
        return header;
    }

    private String createSignature(String timestamp, String token) {
        return Hashing.sha256().hashString(timestamp + token + timestamp, StandardCharsets.UTF_8).toString();
    }
}
