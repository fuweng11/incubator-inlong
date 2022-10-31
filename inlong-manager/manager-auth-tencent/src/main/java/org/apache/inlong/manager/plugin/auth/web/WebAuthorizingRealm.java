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

package org.apache.inlong.manager.plugin.auth.web;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.tencentauth.config.AuthConfig;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.apache.inlong.manager.service.user.UserService;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

/**
 * Web authorizing realm
 */
@Slf4j
public class WebAuthorizingRealm extends AuthorizingRealm {

    private final TofAuthenticator tofAuthenticator;
    private final MockAuthenticator mockAuthenticator;

    public WebAuthorizingRealm(UserService userService, SmartGateService smartGateService, AuthConfig authConfig) {
        this.tofAuthenticator = new TofAuthenticator(userService, smartGateService, authConfig.getAppToken());
        this.mockAuthenticator = new MockAuthenticator(userService, smartGateService);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof MockAuthenticationToken || token instanceof TofAuthenticationToken;
    }

    /**
     * Get authorization info
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principalCollection) {
        SimpleAuthorizationInfo authorizationInfo = new SimpleAuthorizationInfo();
        UserInfo userInfo = (UserInfo) getAvailablePrincipal(principalCollection);
        if (userInfo != null) {
            authorizationInfo.setRoles(userInfo.getRoles());
            LoginUserUtils.getLoginUser().setRoles(authorizationInfo.getRoles());
        }
        return authorizationInfo;
    }

    /**
     * Get authentication info
     */
    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof TofAuthenticationToken) {
            return tofAuthenticator.authenticate(token);
        }
        if (token instanceof MockAuthenticationToken) {
            return mockAuthenticator.authenticate(token);
        }

        throw new AuthenticationException("no authentication");
    }
}
