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
import org.apache.inlong.manager.plugin.auth.web.WebAuthenticationFilter;
import org.apache.inlong.manager.plugin.auth.web.WebAuthorizingRealm;
import org.apache.inlong.manager.plugin.common.beans.AuthConfig;
import org.apache.inlong.manager.plugin.common.enums.Env;
import org.apache.inlong.manager.plugin.service.SmartGateService;
import org.apache.inlong.manager.service.core.RoleService;
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

    @Autowired
    private RoleService roleService;

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
        AuthorizingRealm openAPIRealm = new OpenAPIAuthorizingRealm(roleService, authConfig);
        openAPIRealm.setCredentialsMatcher(getCredentialsMatcher());

        return Arrays.asList(webRealm, openAPIRealm);
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
        filters.put(FILTER_NAME_WEB, new WebAuthenticationFilter(allowMock));
        filters.put(FILTER_NAME_API, new OpenAPIAuthenticationFilter(allowMock, roleService));
        shiroFilterFactoryBean.setFilters(filters);

        // anon: can be accessed by anyone
        Map<String, String> pathDefinitions = new LinkedHashMap<>();

        // swagger api
        pathDefinitions.put("/doc.html", "anon");
        pathDefinitions.put("/v2/api-docs/**/**", "anon");
        pathDefinitions.put("/webjars/**/*", "anon");
        pathDefinitions.put("/swagger-resources/**/*", "anon");
        pathDefinitions.put("/swagger-resources", "anon");

        // open api
        pathDefinitions.put("/openapi/**/*", FILTER_NAME_API);

        // other web
        pathDefinitions.put("/**", FILTER_NAME_WEB);

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
}
