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

import com.google.common.hash.Hashing;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.crypto.DirectDecrypter;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.user.UserService;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Map;

/**
 * TOF authenticator
 */
@Slf4j
public class TofAuthenticator extends BaseProxyAuthenticator {

    private static final String COMMA = ",";

    private final String appToken;

    private final boolean identitySafeMode;

    public TofAuthenticator(UserService userService, SmartGateService smartGateService, String appToken, boolean identitySafeMode) {
        super(userService, smartGateService);
        this.appToken = appToken;
        this.identitySafeMode = identitySafeMode;
    }

    public static Map<String, Object> decodeAuthorizationHeader(String token, String authorizationHeader)
            throws Exception {
        try {
            JWEDecrypter decrypted = new DirectDecrypter(token.getBytes());
            JWEObject jweObject = JWEObject.parse(authorizationHeader);
            jweObject.decrypt(decrypted);
            Map<String, Object> payload = jweObject.getPayload().toJSONObject();
            if (payload != null) {
                String expiration = payload.get("Expiration").toString();
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 7, true)
                        .appendOffsetId()
                        .toFormatter();
                LocalDateTime expirationDate = LocalDateTime.parse(expiration, formatter);
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime before = now.minusMinutes(5);

                if (expirationDate.compareTo(before) < 0) {
                    throw new Exception("expired");
                }
            } else {
                log.info("auth faild result is null ");
                throw new Exception(" auth failed result is null");
            }
            return payload;
        } catch (Exception e) {
            log.info("excption: ", e);
            throw new Exception("excption: " + e.getMessage());
        }
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

        String computedSignature = identitySafeMode ?
                computeSignatureForSafeMode(token.getTimestamp(), token.getRioSeq(),
                        "", "", "")
                : computeSignature(token.getTimestamp(), token.getRioSeq(),
                        token.getUserId(), token.getUsername(), token.getExtData());
        try {
            if (!computedSignature.toUpperCase().equals(token.getSignature())) {
                throw new AuthenticationException("Invalid tof token.");
            } else if (identitySafeMode) {
                Map<String, Object> payload = decodeAuthorizationHeader(appToken, token.getTaiIdentity());
                if (payload != null && !payload.isEmpty()) {
                    token.setUserId(payload.get("StaffId").toString());
                    token.setUsername((String) payload.get("LoginName"));
                    log.info("result: " + token);
                }
            }
        } catch (Exception e) {
            log.error("auth failed :", e);
            throw new AuthenticationException("auth failed" + e.getMessage());
        }
    }

    private String computeSignature(String timestamp, String rioSeq, String userId, String userName, String extData) {

        String builder = timestamp
                + appToken + rioSeq + COMMA
                + userId + COMMA + userName + COMMA
                + extData + timestamp;
        return Hashing.sha256().hashString(builder, StandardCharsets.UTF_8).toString();
    }

    private String computeSignatureForSafeMode(String timestamp, String x_rio_seq, String userId, String userName,
            String x_ext_data) {

        String builder = timestamp
                + appToken + x_rio_seq + COMMA
                + userId + COMMA + userName + COMMA
                + x_ext_data + timestamp;
        return Hashing.sha256().hashString(builder, StandardCharsets.UTF_8).toString();
    }
}
