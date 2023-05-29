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

import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.user.UserService;

import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;

import java.nio.charset.StandardCharsets;

/**
 * TOF authenticator
 */
@Slf4j
public class TofAuthenticator extends BaseProxyAuthenticator {

    private static final String COMMA = ",";

    private final String appToken;

    public TofAuthenticator(UserService userService, SmartGateService smartGateService, String appToken) {
        super(userService, smartGateService);
        this.appToken = appToken;
    }

    @Override
    public String getUserName(AuthenticationToken authenticationToken) {
        checkSignature((TofAuthenticationToken) authenticationToken);
        return ((TofAuthenticationToken) authenticationToken).getUsername();
    }

    @Override
    public AuthenticationType getAuthenticationType() {
        return AuthenticationType.TOF;
    }

    private void checkSignature(TofAuthenticationToken token) {
        if (log.isDebugEnabled()) {
            log.info(token.toString() + ", key = " + this.appToken);
        }

        long now = System.currentTimeMillis();
        if (Math.abs(now / 1000 - Long.parseLong(token.getTimestamp())) > 180) {
            throw new AuthenticationException("Tof token expired.");
        }
        String computedSignature = computeSignature(token.getTimestamp(), token.getRioSeq(),
                token.getUserId(), token.getUsername(), token.getExtData());
        if (!computedSignature.toUpperCase().equals(token.getSignature())) {
            throw new AuthenticationException("Invalid tof token.");
        }
    }

    private String computeSignature(String timestamp, String rioSeq, String userId, String userName, String extData) {

        String builder = timestamp
                + appToken + rioSeq + COMMA
                + userId + COMMA + userName + COMMA
                + extData + timestamp;
        return Hashing.sha256().hashString(builder, StandardCharsets.UTF_8).toString();
    }
}
