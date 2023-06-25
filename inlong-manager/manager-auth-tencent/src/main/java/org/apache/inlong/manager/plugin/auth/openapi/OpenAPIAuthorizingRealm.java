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

import org.apache.inlong.manager.common.enums.TenantUserTypeEnum;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.TenantRoleInfo;
import org.apache.inlong.manager.pojo.user.TenantRolePageRequest;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.tencentauth.config.AuthConfig;
import org.apache.inlong.manager.service.user.TenantRoleService;
import org.apache.inlong.manager.service.user.UserService;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Open api authorizing realm
 */
@Slf4j
public class OpenAPIAuthorizingRealm extends AuthorizingRealm {

    private final TAuthAuthenticator tAuthAuthenticator;
    private final UserService userService;
    private final TenantRoleService tenantRoleService;

    public OpenAPIAuthorizingRealm(UserService userService, TenantRoleService tenantRoleService,
            AuthConfig authConfig) {
        this.userService = userService;
        this.tenantRoleService = tenantRoleService;
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
        SimpleAuthorizationInfo authorizationInfo = new SimpleAuthorizationInfo();
        Object principal = getAvailablePrincipal(principalCollection);
        if (principal == null) {
            log.warn("null principal");
            return authorizationInfo;
        }
        if (principal instanceof UserInfo) {
            UserInfo userInfo = (UserInfo) principal;
            authorizationInfo.setRoles(userInfo.getRoles());
            LoginUserUtils.getLoginUser().setRoles(authorizationInfo.getRoles());
            LoginUserUtils.getLoginUser().setAccountType(userInfo.getAccountType());
            return authorizationInfo;
        }
        if (principal instanceof String) {
            UserInfo userInfoDB = userService.getByName((String) principal);
            TenantRolePageRequest tenantRolePageRequest = new TenantRolePageRequest();
            tenantRolePageRequest.setUsername((String) principal);
            List<String> roles = tenantRoleService.listByCondition(tenantRolePageRequest).getList().stream().map(
                    TenantRoleInfo::getUsername).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(roles)) {
                authorizationInfo.setRoles(Sets.newHashSet(UserRoleCode.TENANT_OPERATOR));
            } else {
                authorizationInfo.setRoles(Sets.newHashSet(roles));
            }
            LoginUserUtils.getLoginUser().setRoles(authorizationInfo.getRoles());
            // add account type info
            if (userInfoDB == null) {
                LoginUserUtils.getLoginUser().setAccountType(TenantUserTypeEnum.TENANT_OPERATOR.getCode());
            } else {
                LoginUserUtils.getLoginUser().setAccountType(userInfoDB.getAccountType());
            }
        }

        log.warn("principal {} not instance of UserInfo nor String, ignored", principal);
        return authorizationInfo;
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
