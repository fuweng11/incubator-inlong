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

package org.apache.inlong.manager.client.api.service;

import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

/**
 * AuthInterceptor
 * Before okhttp call a request, uniformly encapsulate the relevant parameters of authentication
 */
public class AuthInterceptor implements Interceptor {

    private static final String MANAGER_CLIENT_REQUEST = "Manager_Client_Request";
    private static final String MANAGER_SECURE_AUTH = "secure-authentication";
    private static final String MANAGER_NAME = "inlong_manager";

    private final String username;
    private final String password;

    public AuthInterceptor(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        TauthClient tauthClient = new TauthClient(username, password);
        try {
            String encodedAuthentication = tauthClient.getAuthentication(MANAGER_NAME);
            Request oldRequest = chain.request();
            HttpUrl.Builder builder = oldRequest.url()
                    .newBuilder()
                    .addEncodedQueryParameter("username", username)
                    .addEncodedQueryParameter("password", password);

            Request newRequest = oldRequest.newBuilder()
                    .method(oldRequest.method(), oldRequest.body())
                    .addHeader(MANAGER_CLIENT_REQUEST, "true")
                    .addHeader(MANAGER_SECURE_AUTH, encodedAuthentication)
                    .url(builder.build())
                    .build();

            return chain.proceed(newRequest);
        } catch (SecureException e) {
            throw new IllegalStateException("failed to request with tauth, ex=" + e.getMessage());
        }
    }
}
