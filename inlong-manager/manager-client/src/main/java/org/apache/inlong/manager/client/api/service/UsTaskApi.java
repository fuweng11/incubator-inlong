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

import org.apache.inlong.manager.pojo.common.Response;

import retrofit2.Call;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface UsTaskApi {

    @POST("usTask/create/{id}")
    Call<Response<String>> create(@Path("id") Integer id);

    @POST("usTask/freeze/{id}")
    Call<Response<Boolean>> freeze(@Path("id") Integer id);

}
