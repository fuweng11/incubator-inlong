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
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.NetworkUtils;
import org.apache.inlong.manager.plugin.auth.openapi.BasicAuthenticationToken;
import org.apache.inlong.manager.plugin.auth.openapi.TAuthAuthenticationToken;
import org.apache.inlong.manager.plugin.common.pojo.user.StaffDTO;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.service.core.RoleService;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.subject.SimplePrincipalCollection;
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
import java.util.Optional;

/**
 * Web authentication filter
 */
@Slf4j
public class WebAuthenticationFilter implements Filter {

    private static final String MANAGER_CLIENT_REQUEST = "Manager_Client_Request";

    private final boolean supportMockUsername;

    private final RoleService roleService;

    public WebAuthenticationFilter(boolean supportMockUsername, RoleService roleService) {
        if (supportMockUsername) {
            log.warn("ensure that you are not using the test mode in production environment");
        }
        this.roleService = roleService;
        this.supportMockUsername = supportMockUsername;
    }

    @Override
    public void init(FilterConfig filterConfig) {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        if (isManagerClientRequest(request) && doTAuthFilter(servletRequest, servletResponse, filterChain)) {
            log.info("login manager client success");
            return;
        }

        ProxyUserAuthenticationToken token = getAuthenticationToken(request);
        String clientIp = NetworkUtils.getClientIpAddress(request);
        if (token == null) {
            log.error("access denied for anonymous user with no token, clientIp: {}, path: {}", clientIp,
                    request.getServletPath());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        Subject subject = SecurityUtils.getSubject();
        if (subject.isAuthenticated() && !isMock(token) && !isProxy(token)) {
            UserInfo userInfo = (UserInfo) subject.getPrincipal();
            StaffDTO staffInfo = StaffDTO.getFromJson(userInfo.getExtParams());
            if (staffInfo == null || StringUtils.isBlank(staffInfo.getProxyUser())) {
                doFilter(servletRequest, servletResponse, filterChain, userInfo);
                return;
            }
        }

        try {
            subject.login(token);
        } catch (AuthenticationException e) {
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage());
            return;
        }

        log.info("login user: " + subject.getPrincipal() + ", ip: " + clientIp
                + ", token " + token.getClass().getSimpleName());
        if (!subject.isAuthenticated()) {
            log.error("access denied for anonymous user: {}, clientIp: {}, path: {}", subject.getPrincipal(),
                    clientIp, request.getServletPath());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        UserInfo userInfo = (UserInfo) subject.getPrincipal();
        StaffDTO staffInfo = StaffDTO.getFromJson(userInfo.getExtParams());
        if (staffInfo != null && StringUtils.isNotBlank(staffInfo.getProxyUser())) {
            String realmName = Optional.ofNullable(subject.getPrincipals().getRealmNames())
                    .orElseThrow(
                            () -> new BusinessException(ErrorCodeEnum.AUTHORIZATION_FAILED, "get proxy info failed"))
                    .iterator().next();
            subject.runAs(new SimplePrincipalCollection(token.getProxyUser(), realmName));
        }

        doFilter(servletRequest, servletResponse, filterChain, userInfo);
    }

    private void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain,
            UserInfo userInfo) throws IOException, ServletException {
        LoginUserUtils.setUserLoginInfo(userInfo);
        try {
            filterChain.doFilter(servletRequest, servletResponse);
        } finally {
            LoginUserUtils.removeUserLoginInfo();
        }
    }

    private boolean isProxy(ProxyUserAuthenticationToken token) {
        return token != null && token.isProxy();
    }

    private boolean isMock(ProxyUserAuthenticationToken token) {
        return token instanceof MockAuthenticationToken;
    }

    private ProxyUserAuthenticationToken getAuthenticationToken(HttpServletRequest httpServletRequest) {
        MockAuthenticationToken mockAuthenticationToken = new MockAuthenticationToken(httpServletRequest);
        if (supportMockUsername && (!mockAuthenticationToken.isEmpty())) {
            return mockAuthenticationToken;
        }

        TofAuthenticationToken tofAuthenticationToken = new TofAuthenticationToken(httpServletRequest);
        if (!tofAuthenticationToken.isEmpty()) {
            return tofAuthenticationToken;
        }
        return null;
    }

    private boolean isManagerClientRequest(HttpServletRequest request) {
        String isManagerClientRequest = request.getHeader(MANAGER_CLIENT_REQUEST);
        if (StringUtils.isNotBlank(isManagerClientRequest)) {
            log.info("It's the manager client request, use mock authentication");
            return true;
        }
        return false;
    }

    private boolean doTAuthFilter(
            ServletRequest servletRequest,
            ServletResponse servletResponse,
            FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;

        TAuthAuthenticationToken tAuthAuthenticationToken = new TAuthAuthenticationToken(request);
        BasicAuthenticationToken token = tAuthAuthenticationToken;

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
                return false;
            }
        }

        if (!subject.isAuthenticated()) {
            log.error("access denied for anonymous user: {}, clientIp: {}, path: {} ", subject.getPrincipal(),
                    clientIp, request.getServletPath());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
            return false;
        }

        UserInfo userInfo = new UserInfo();
        String userName = (String) (subject.getPrincipal());
        List<String> roleList = roleService.listByUser(userName);
        userInfo.setName(userName);
        userInfo.setRoles(new HashSet<>(roleList));
        LoginUserUtils.setUserLoginInfo(userInfo);

        filterChain.doFilter(servletRequest, servletResponse);
        LoginUserUtils.removeUserLoginInfo();
        return true;
    }

    @Override
    public void destroy() {

    }
}
