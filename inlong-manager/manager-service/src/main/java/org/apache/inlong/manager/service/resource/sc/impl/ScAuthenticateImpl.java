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

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.service.resource.sc.ScAuthenticate;
import org.apache.inlong.manager.service.tencentauth.config.AuthConfig;

import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.ServiceTarget;
import com.tencent.tdw.security.authentication.client.SecureClient;
import com.tencent.tdw.security.authentication.client.SecureClientFactory;
import com.tencent.tdw.security.exceptions.SecureException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * security center authentication service
 */
@Slf4j
@Service
public class ScAuthenticateImpl implements ScAuthenticate {

    private final SecureClient secureClient;

    @Autowired
    public ScAuthenticateImpl(AuthConfig authConfig) {
        secureClient = SecureClientFactory.generateTAuthSecureClient(authConfig.getAccount(),
                LocalKeyManager.generateByDefaultKey(authConfig.getCmk()));
    }

    @Override
    public String tauth(String target) {
        return tauth(target, null);
    }

    @Override
    public String tauth(String target, String proxy) {
        Authentication authentication;
        try {
            authentication = secureClient.getAuthentication(ServiceTarget.valueOf(target));
        } catch (SecureException e) {
            log.error("tauth get authentication failed, target: {}, proxy: {}", target, proxy, e);
            throw new BusinessException(ErrorCodeEnum.SC_AUTHENTICATE_FAILED);
        }
        String tauth = authentication.flat();
        if (log.isDebugEnabled()) {
            log.debug("get tauth target: {}, proxy: {}, token: {}", target, proxy, tauth);
        }
        return tauth;
    }

    @Override
    public String urlEncode(String sourceToken) {
        String token;
        try {
            token = URLEncoder.encode(sourceToken, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("tauth url encode failed, sourceToken: {} ", sourceToken, e);
            throw new BusinessException(ErrorCodeEnum.SC_AUTHENTICATE_FAILED);
        }
        return token;
    }

}
