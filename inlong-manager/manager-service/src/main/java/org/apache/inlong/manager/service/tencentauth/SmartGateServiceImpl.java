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

package org.apache.inlong.manager.service.tencentauth;

import org.apache.inlong.manager.common.util.JsonUtils;

import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * SmartGateServiceImpl
 */
@Service
public class SmartGateServiceImpl implements SmartGateService {

    @Autowired
    private SmartGateApiRequestService smartGateApiRequestService;

    @Override
    public StaffBaseInfo getStaffByEnName(String enName) {
        Map<String, Object> params = Maps.newHashMap();
        params.put("engName", enName);
        String response = smartGateApiRequestService
                .getCall("getStaffInfoByEngName", params, new ParameterizedTypeReference<String>() {
                });
        return JsonUtils.parseObject(response, StaffBaseInfo.class);
    }
}
