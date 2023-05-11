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

package org.apache.inlong.manager.pojo.tencent.us;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Us configuration information
 */
@Data
@Component
@ConfigurationProperties(prefix = "inlong.us")
public class USConfiguration {

    // address of us service
    private String apiUrl;

    private String securityUrl;
    private String defaultSuperSqlUrl;
    private String serviceName;
    private String proxyUser;
    private String cmk;

    private String jdbcDriver;
    private String jdbcUrl;
    private String jdbcUsername;
    private String jdbcPassword;

    private String querySql;
    private String binLogGap;

    private String partEndDelayMinute;

    private String managerlistServerUrl;

    @Value("#{${inlong.us.appGroupToSuperSQL:{'jdbc:supersql':'url=http://ss-qe-supersql-ultra.tencent-distribute.com:8081'}}}")
    private Map<String, String> superSqlUrlMap = new HashMap<>();

}
