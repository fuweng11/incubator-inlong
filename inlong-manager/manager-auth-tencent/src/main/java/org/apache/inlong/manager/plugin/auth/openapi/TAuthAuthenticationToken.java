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

import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;

import com.tencent.tdw.security.authentication.Authentication;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletRequest;

import java.net.URLDecoder;

/**
 * TAuth authentication token
 */
@Slf4j
public class TAuthAuthenticationToken implements BasicAuthenticationToken {

    // verify login by tdw signature
    public static final String HEADER_TDW_TOKEN = "secure-authentication";

    private Authentication authentication;

    public TAuthAuthenticationToken(HttpServletRequest request) {
        String rawAuthentication = request.getHeader(HEADER_TDW_TOKEN);
        try {
            if (rawAuthentication != null) {
                String rawAuth = URLDecoder.decode(rawAuthentication, "UTF-8");
                this.authentication = Authentication.valueOf(rawAuthentication);
            }
        } catch (Exception e) {
            log.error("TDW token authentication; HEADER_TDW_TOKEN={}", rawAuthentication, e);
        }
    }

    public boolean isEmpty() {
        return this.authentication == null;
    }

    @Override
    public Object getPrincipal() {
        return authentication;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public AuthenticationType getType() {
        return AuthenticationType.TAUTH;
    }
}
