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

package org.apache.inlong.manager.plugin.auth.openapi;

import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.service.SecureService;
import com.tencent.tdw.security.authentication.service.SecureServiceFactory;
import lombok.SneakyThrows;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.Authenticator;
import org.apache.shiro.authc.SimpleAuthenticationInfo;

import java.util.Optional;

/**
 * TAuth authenticator
 */
public class TAuthAuthenticator implements Authenticator {

    private final SecureService secureService;

    public TAuthAuthenticator(String service, String key) {
        secureService = SecureServiceFactory.getOrCreate(
                new SecureServiceFactory.ServiceConf(service, null, LocalKeyManager.generateByDefaultKey(key)));
    }

    @SneakyThrows
    @Override
    public AuthenticationInfo authenticate(AuthenticationToken authenticationToken) throws AuthenticationException {
        TAuthAuthenticationToken token = (TAuthAuthenticationToken) authenticationToken;
        Authentication authentication = (Authentication) token.getPrincipal();
        com.tencent.tdw.security.authentication.Authenticator authenticator = secureService.authenticate(
                authentication);
        return new SimpleAuthenticationInfo(authenticator.getUser(), null,
                Optional.ofNullable(authenticator.getRealUser()).orElse(authenticator.getUser()));
    }

}
