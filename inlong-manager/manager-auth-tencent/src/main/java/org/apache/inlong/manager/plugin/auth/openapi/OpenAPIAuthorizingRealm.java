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

import com.google.common.collect.Sets;
import org.apache.inlong.manager.service.tencentauth.bean.AuthConfig;
import org.apache.inlong.manager.service.core.RoleService;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

/**
 * Open api authorizing realm
 */
public class OpenAPIAuthorizingRealm extends AuthorizingRealm {

    private final TAuthAuthenticator tAuthAuthenticator;
    private final RoleService roleService;

    public OpenAPIAuthorizingRealm(RoleService roleService, AuthConfig authConfig) {
        this.roleService = roleService;
        this.tAuthAuthenticator = new TAuthAuthenticator(authConfig.getService(), authConfig.getSmk());
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof MockAuthenticationToken || token instanceof TAuthAuthenticationToken;
    }

    /**
     * Get authorization info
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principalCollection) {
        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo();
        String username = (String) getAvailablePrincipal(principalCollection);
        if (username != null) {
            info.setRoles(Sets.newHashSet(roleService.listByUser(username)));
            LoginUserUtils.getLoginUser().setRoles(info.getRoles());
        }
        return info;
    }

    /**
     * Get authentication info
     */
    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof TAuthAuthenticationToken) {
            return tAuthAuthenticator.authenticate(token);
        }
        String username = (String) token.getPrincipal();
        if (token instanceof MockAuthenticationToken) {
            return new SimpleAuthenticationInfo(username, "", username);
        }

        throw new AuthenticationException("no authentication");
    }
}
