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

package org.apache.inlong.dataproxy.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.inlong.dataproxy.config.AuthUtils;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.apache.inlong.dataproxy.consts.ConfigConstants.DEFAULT_INTER_MANAGER_NAME;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.DEFAULT_INTER_MANAGER_SECURE_AUTH;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.DEFAULT_INTER_NAMANGER_USER_KEY;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.DEFAULT_INTER_NAMANGER_USER_NAME;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.INTER_MANAGER_NAME;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.INTER_MANAGER_SECURE_AUTH;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.INTER_NAMANGER_USER_KEY;
import static org.apache.inlong.dataproxy.consts.ConfigConstants.INTER_NAMANGER_USER_NAME;

public class HttpUtils {

    public static final String APPLICATION_JSON = "application/json";
    private static final Gson GSON = new GsonBuilder().create();

    public static StringEntity getEntity(Object obj) throws UnsupportedEncodingException {
        StringEntity se = new StringEntity(GSON.toJson(obj));
        se.setContentType(APPLICATION_JSON);
        return se;
    }

    public static HttpPost getHttPost(String url) throws SecureException {
        Map<String, String> properties = CommonConfigHolder.getInstance().getProperties();
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader(HttpHeaders.CONNECTION, "close");
        httpPost.addHeader(HttpHeaders.AUTHORIZATION, AuthUtils.genBasicAuth());

        // internal secure-auth
        String secureAuth = properties.getOrDefault(INTER_MANAGER_SECURE_AUTH, DEFAULT_INTER_MANAGER_SECURE_AUTH);
        String serviceName = properties.getOrDefault(INTER_MANAGER_NAME, DEFAULT_INTER_MANAGER_NAME);
        String secureUserName = properties.getOrDefault(INTER_NAMANGER_USER_NAME, DEFAULT_INTER_NAMANGER_USER_NAME);
        String secureUserKey = properties.getOrDefault(INTER_NAMANGER_USER_KEY, DEFAULT_INTER_NAMANGER_USER_KEY);
        if (StringUtils.isNotBlank(secureUserName) && StringUtils.isNotBlank(secureUserKey)) {
            TauthClient tauthClient = new TauthClient(secureUserName, secureUserKey);
            String encodedAuthentication = tauthClient.getAuthentication(serviceName);
            httpPost.addHeader(secureAuth, encodedAuthentication);
        }
        return httpPost;

    }

}
