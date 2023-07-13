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

package org.apache.inlong.manager.plugin.auth;

import org.apache.inlong.manager.common.auth.InlongShiro;
import org.apache.inlong.manager.plugin.auth.openapi.OpenAPIAuthenticationFilter;
import org.apache.inlong.manager.plugin.auth.openapi.OpenAPIAuthorizingRealm;
import org.apache.inlong.manager.plugin.auth.tenant.TenantAuthenticatingFilter;
import org.apache.inlong.manager.plugin.auth.tenant.TenantAuthenticatingRealm;
import org.apache.inlong.manager.plugin.auth.web.WebAuthenticationFilter;
import org.apache.inlong.manager.plugin.auth.web.WebAuthorizingRealm;
import org.apache.inlong.manager.plugin.common.enums.Env;
import org.apache.inlong.manager.service.tenant.InlongTenantService;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.tencentauth.config.AuthConfig;
import org.apache.inlong.manager.service.user.InlongRoleService;
import org.apache.inlong.manager.service.user.TenantRoleService;
import org.apache.inlong.manager.service.user.UserService;

import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.spring.security.interceptor.AuthorizationAttributeSourceAdvisor;
import org.apache.shiro.spring.web.ShiroFilterFactoryBean;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.shiro.web.mgt.WebSecurityManager;
import org.apache.shiro.web.session.mgt.DefaultWebSessionManager;
import org.apache.shiro.web.session.mgt.WebSessionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.servlet.Filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shiro configuration provider plugin implementation
 */
@Component
@ConditionalOnProperty(name = "type", prefix = "inlong.auth", havingValue = "tencent")
public class InlongShiroImpl implements InlongShiro {

    private static final String FILTER_NAME_WEB = "authWeb";
    private static final String FILTER_NAME_API = "authAPI";
    private static final String FILTER_NAME_TENANT = "authTenant";

    @Value("${openapi.auth.enabled:false}")
    private Boolean openAPIAuthEnabled;

    @Autowired
    private InlongRoleService inlongRoleService;

    @Autowired
    private TenantRoleService tenantRoleService;

    @Autowired
    private InlongTenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private SmartGateService smartGateService;

    @Autowired
    private AuthConfig authConfig;

    @Value("${spring.profiles.active}")
    private String env;

    @Override
    public WebSecurityManager getWebSecurityManager() {
        return new DefaultWebSecurityManager();
    }

    @Override
    public Collection<Realm> getShiroRealms() {
        // web realm
        AuthorizingRealm webRealm = new WebAuthorizingRealm(userService, smartGateService, authConfig);
        webRealm.setCredentialsMatcher(getCredentialsMatcher());

        // openAPI realm
        AuthorizingRealm openAPIRealm = new OpenAPIAuthorizingRealm(userService, tenantRoleService, authConfig);
        openAPIRealm.setCredentialsMatcher(getCredentialsMatcher());

        Realm tenantRealm = new TenantAuthenticatingRealm(tenantRoleService, inlongRoleService,
                userService, tenantService);
        return Arrays.asList(webRealm, openAPIRealm, tenantRealm);
    }

    @Override
    public WebSessionManager getWebSessionManager() {
        return new DefaultWebSessionManager();
    }

    @Override
    public CredentialsMatcher getCredentialsMatcher() {
        return (authenticationToken, authenticationInfo) -> true;
    }

    @Override
    public ShiroFilterFactoryBean getShiroFilter(SecurityManager securityManager) {
        ShiroFilterFactoryBean shiroFilterFactoryBean = new ShiroFilterFactoryBean();
        shiroFilterFactoryBean.setSecurityManager(securityManager);

        Map<String, Filter> filters = new LinkedHashMap<>();
        boolean allowMock = !Env.PROD.equals(Env.forName(env));
        filters.put(FILTER_NAME_WEB, new WebAuthenticationFilter(allowMock, userService, tenantRoleService));
        shiroFilterFactoryBean.setFilters(filters);

        // anon: can be accessed by anyone
        Map<String, String> pathDefinitions = new LinkedHashMap<>();

        // swagger api
        pathDefinitions.put("/doc.html", "anon");
        pathDefinitions.put("/v2/api-docs/**/**", "anon");
        pathDefinitions.put("/webjars/**/*", "anon");
        pathDefinitions.put("/swagger-resources/**/*", "anon");
        pathDefinitions.put("/swagger-resources", "anon");

        // openapi
        filters.put(FILTER_NAME_API, new OpenAPIAuthenticationFilter(allowMock, userService, tenantRoleService));
        pathDefinitions.put("/openapi/**/*", genFiltersInOrder(FILTER_NAME_API, FILTER_NAME_TENANT));

        // other web
        pathDefinitions.put("/**", genFiltersInOrder(FILTER_NAME_WEB, FILTER_NAME_TENANT));

        // tenant filter
        filters.put(FILTER_NAME_TENANT, new TenantAuthenticatingFilter());

        shiroFilterFactoryBean.setFilterChainDefinitionMap(pathDefinitions);
        return shiroFilterFactoryBean;
    }

    @Override
    public AuthorizationAttributeSourceAdvisor getAuthorizationAttributeSourceAdvisor(SecurityManager securityManager) {
        AuthorizationAttributeSourceAdvisor authorizationAttributeSourceAdvisor =
                new AuthorizationAttributeSourceAdvisor();
        authorizationAttributeSourceAdvisor.setSecurityManager(securityManager);
        return authorizationAttributeSourceAdvisor;
    }

    private String genFiltersInOrder(String... filterNames) {
        if (filterNames.length == 1) {
            return filterNames[0];
        }

        StringBuilder builder = new StringBuilder();
        for (String filterName : filterNames) {
            builder.append(filterName).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
}
