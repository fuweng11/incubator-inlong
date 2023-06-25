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
import org.apache.inlong.manager.common.util.NetworkUtils;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.TenantRoleInfo;
import org.apache.inlong.manager.pojo.user.TenantRolePageRequest;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.service.user.TenantRoleService;
import org.apache.inlong.manager.service.user.UserService;

import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Open api authentication filter
 */
@Slf4j
public class OpenAPIAuthenticationFilter implements Filter {

    private final boolean supportMockUsername;

    private final UserService userService;
    private final TenantRoleService tenantRoleService;

    public OpenAPIAuthenticationFilter(boolean supportMockUsername, UserService userService,
            TenantRoleService tenantRoleService) {
        if (supportMockUsername) {
            log.warn("Ensure that you are not using test mode in production environment.");
        }
        this.supportMockUsername = supportMockUsername;
        this.userService = userService;
        this.tenantRoleService = tenantRoleService;
    }

    @Override
    public void init(FilterConfig filterConfig) {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;

        MockAuthenticationToken mockAuthenticationToken = new MockAuthenticationToken(request);
        TAuthAuthenticationToken tAuthAuthenticationToken = new TAuthAuthenticationToken(request);

        BasicAuthenticationToken token = null;
        if (supportMockUsername && (!mockAuthenticationToken.isEmpty())) {
            token = mockAuthenticationToken;
        } else if (!tAuthAuthenticationToken.isEmpty()) {
            token = tAuthAuthenticationToken;
        }

        // 403 on login failure
        String clientIp = NetworkUtils.getClientIpAddress(request);
        Subject subject = SecurityUtils.getSubject();
        if (token != null) {
            try {
                subject.login(token);
                log.debug("login user: " + subject.getPrincipal() + ", clientIp: " + clientIp
                        + ", token " + token.getClass().getSimpleName());
            } catch (Exception e) {
                ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage());
                return;
            }
        }

        if (!subject.isAuthenticated()) {
            log.error("access denied for anonymous user: {}, clientIp: {}, path: {} ", subject.getPrincipal(),
                    clientIp, request.getServletPath());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        UserInfo userInfo = new UserInfo();
        String userName = (String) (subject.getPrincipal());
        UserInfo userInfoDB = userService.getByName(userName);
        TenantRolePageRequest tenantRolePageRequest = new TenantRolePageRequest();
        tenantRolePageRequest.setUsername(userName);
        List<String> roles = tenantRoleService.listByCondition(tenantRolePageRequest).getList().stream().map(
                TenantRoleInfo::getUsername).collect(Collectors.toList());
        userInfo.setName(userName);
        userInfo.setRoles(new HashSet<>(roles));
        // add account type info
        if (userInfoDB == null) {
            userInfo.setAccountType(TenantUserTypeEnum.TENANT_OPERATOR.getCode());
        } else {
            userInfo.setAccountType(userInfoDB.getAccountType());
        }
        LoginUserUtils.setUserLoginInfo(userInfo);

        filterChain.doFilter(servletRequest, servletResponse);
        LoginUserUtils.removeUserLoginInfo();
    }

    @Override
    public void destroy() {

    }
}
