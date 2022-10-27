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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;
import org.apache.shiro.authc.AuthenticationToken;

import javax.servlet.http.HttpServletRequest;

/**
 * Proxy user authentication token
 */
public interface ProxyUserAuthenticationToken extends AuthenticationToken {

    String HEADER_PROXY_USER = "secure-proxy-user";

    /**
     * Get authentication type
     */
    AuthenticationType getType();

    /**
     * Get username
     */
    String getUsername();

    /**
     * Get proxy user (real user)
     */
    String getProxyUser();

    /**
     * Is proxy user or not
     */
    default boolean isProxy() {
        return !StringUtils.isEmpty(getProxyUser());
    }

    /**
     * Get proxy user from request header
     */
    default String getProxyUserFromHeader(HttpServletRequest request) {
        return request.getHeader(HEADER_PROXY_USER);
    }
}
