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

package org.apache.inlong.dataproxy.config;

import org.apache.inlong.common.util.BasicAuth;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.HmacUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.SecureRandom;

public class AuthUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);

    /**
     * Generate http basic auth credential from configured secretId and secretKey
     */
    public static String genBasicAuth() {
        return BasicAuth.genBasicAuthCredential(
                CommonConfigHolder.getInstance().getManagerAuthSecretId(),
                CommonConfigHolder.getInstance().getManagerAuthSecretKey());
    }

    public static String genTDBankAuthToken(String userName, String usrPassWord) {
        long timestamp = System.currentTimeMillis();
        int nonce =
                new SecureRandom(StringUtils.getBytesUtf8(String.valueOf(timestamp))).nextInt(Integer.MAX_VALUE);
        String signature = getAuthSignature(userName, usrPassWord, timestamp, nonce);
        return "TDBANK" + " " + userName + " " + timestamp + " " + nonce + " " + signature;
    }

    private static String getAuthSignature(String usrName,
            String usrPassWord, long timestamp, int randomValue) {
        Base64 base64 = new Base64();
        StringBuilder sbuf = new StringBuilder(512);
        byte[] baseStr =
                base64.encode(HmacUtils.hmacSha1(usrPassWord,
                        sbuf.append(usrName).append(timestamp).append(randomValue).toString()));
        sbuf.delete(0, sbuf.length());
        String signature = "";
        try {
            signature = URLEncoder.encode(new String(baseStr, "UTF-8"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.error("Encode signature exception caught", e);
        }
        return signature;
    }

}
