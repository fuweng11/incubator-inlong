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
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;

/**
 * TOF authentication token
 */
public class TofAuthenticationToken implements ProxyUserAuthenticationToken {

    public static final String HEADER_USER_NAME = "staffname";
    public static final String HEADER_SIGNATURE = "signature";
    public static final String HEADER_USER_ID = "staffid";
    public static final String HEADER_TIMESTAMP = "timestamp";
    public static final String HEADER_X_RIO_SEQ = "x-rio-seq";

    /**
     * Reserved header field to store current staff's extension info
     * PC: None, WeChat: OpenID
     */
    public static final String HEADER_X_EXT_DATA = "x-ext-data";

    private String userId;
    private String username;
    private String signature;
    private String timestamp;
    private String rioSeq;
    private String extData;
    private String proxyUser;

    public TofAuthenticationToken(HttpServletRequest request) {
        this.userId = request.getHeader(HEADER_USER_ID);
        this.username = request.getHeader(HEADER_USER_NAME);
        this.signature = request.getHeader(HEADER_SIGNATURE);
        this.timestamp = request.getHeader(HEADER_TIMESTAMP);
        this.rioSeq = request.getHeader(HEADER_X_RIO_SEQ);
        this.extData = request.getHeader(HEADER_X_EXT_DATA);
        extData = StringUtils.isEmpty(extData) ? "" : extData;
        this.proxyUser = getProxyUserFromHeader(request);
    }

    public TofAuthenticationToken() {
    }

    @Override
    public Object getPrincipal() {
        return username;
    }

    @Override
    public Object getCredentials() {
        return signature;
    }

    public boolean isEmpty() {
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(signature)
                || StringUtils.isEmpty(timestamp)) {
            return true;
        }
        return false;
    }

    public String getUserId() {
        return userId;
    }

    @Autowired
    public String getUsername() {
        return username;
    }

    public String getSignature() {
        return signature;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getRioSeq() {
        return rioSeq;
    }

    public String getExtData() {
        return extData;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        str.append("TofAuthenticationToken: userId = " + userId);
        str.append(", userName = " + username);
        str.append(", timestamp = " + timestamp);
        str.append(", x_rio_seq = " + rioSeq);
        str.append(", x_ext_data = " + extData);
        str.append(", signature = " + signature);

        return str.toString();
    }

    @Override
    public String getProxyUser() {
        return this.proxyUser;
    }

    @Override
    public AuthenticationType getType() {
        return AuthenticationType.TOF;
    }
}
