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

package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.UsTaskApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.pojo.common.Response;

public class UsTaskClient {

    private final UsTaskApi usTaskApi;

    public UsTaskClient(ClientConfiguration configuration) {
        usTaskApi = ClientUtils.createRetrofit(configuration).create(UsTaskApi.class);
    }

    /**
     * create us task by sink id
     *
     * @return task id
     */
    public String createUsTaskBySink(Integer sinkId) {
        Response<String> response = ClientUtils.executeHttpCall(usTaskApi.create(sinkId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * freeze us task by sink id
     *
     * @return true or false
     */
    public Boolean freezeUsTaskBySink(Integer sinkId) {
        Response<Boolean> response = ClientUtils.executeHttpCall(usTaskApi.freeze(sinkId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }
}
