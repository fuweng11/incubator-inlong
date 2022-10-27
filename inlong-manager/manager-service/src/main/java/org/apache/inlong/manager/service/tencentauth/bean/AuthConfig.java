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

package org.apache.inlong.manager.service.tencentauth.bean;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Tencent auth configuration
 */
@Data
@Component
@ConfigurationProperties(prefix = "inlong.authc")
public class AuthConfig {

    private String service;
    private String smk;
    private String account;
    private String cmk;
    private String tofKey;

    public String getService() {
        return service;
    }

    public AuthConfig setService(String service) {
        this.service = service;
        return this;
    }

    public String getSmk() {
        return smk;
    }

    public AuthConfig setSmk(String smk) {
        this.smk = smk;
        return this;
    }

    public String getAccount() {
        return account;
    }

    public AuthConfig setAccount(String account) {
        this.account = account;
        return this;
    }

    public String getCmk() {
        return cmk;
    }

    public AuthConfig setCmk(String cmk) {
        this.cmk = cmk;
        return this;
    }

    public String getTofKey() {
        return tofKey;
    }

    public AuthConfig setTofKey(String tofKey) {
        this.tofKey = tofKey;
        return this;
    }
}
