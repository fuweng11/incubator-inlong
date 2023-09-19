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

package org.apache.inlong.manager.service.resource.sink.tencent.pulsar;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class InnerPulsarResourceConfig {

    @Value("${pulsar.manager.app.id}")
    private String appId;
    @Value("${pulsar.manager.app.secret}")
    private String appSecret;
    @Value("${pulsar.manager.url}")
    private String pulsarManagerUrl;
    /**
     * The pulsar manager create topic path
     */
    public static final String CREATE_TOPIC_PATH = "/pulsar-manager/openapi/topic/create";

    /**
     *
     * The pulsar manager create pulsar tenant path
     */
    public static final String CREATE_TENANT_PATH = "/pulsar-manager/openapi/tenant/create";

    /**
     *
     * The pulsar manager create pulsar namespace path
     */
    public static final String CREATE_NAMESPACE_PATH = "/pulsar-manager/openapi/namespace/create";
}
