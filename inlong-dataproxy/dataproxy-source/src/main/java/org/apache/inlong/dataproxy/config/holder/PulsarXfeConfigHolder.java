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

package org.apache.inlong.dataproxy.config.holder;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Save Pulsar transfer configure info
 */
public class PulsarXfeConfigHolder extends PropertiesHolder {

    // each line is {groupId}/{streamId} = {pulsar topic}
    private static final String pulsarTransferFileName = "pulsar_transfer.properties";
    private static final Logger LOG = LoggerFactory.getLogger(PulsarXfeConfigHolder.class);

    public PulsarXfeConfigHolder() {
        super(pulsarTransferFileName);
    }

    public boolean isRequirePulsarTransfer(String groupId, String streamId) {
        if (StringUtils.isEmpty(groupId) || StringUtils.isEmpty(streamId)) {
            return false;
        }
        String key = groupId + "/" + streamId;
        return StringUtils.isNotEmpty(confHolder.get(key));
    }

    public Map<String, String> getPulsarTransferConfigMap() {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : confHolder.entrySet()) {
            if (entry == null
                    || StringUtils.isEmpty(entry.getKey())
                    || StringUtils.isEmpty(entry.getValue())) {
                continue;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    protected Map<String, String> filterInValidRecords(Map<String, String> configMap) {
        Map<String, String> filteredMap = new HashMap<>(configMap.size());
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            if (entry == null
                    || StringUtils.isBlank(entry.getKey())
                    || StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            filteredMap.put(entry.getKey().trim(), entry.getValue().trim());
        }
        return filteredMap;
    }

    @Override
    protected boolean updateCacheData() {
        return true;
    }
}
