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

package org.apache.inlong.dataproxy.config.pojo;

import java.util.List;

public class TDBankMetaConfig {

    private boolean result;
    private List<ConfigItem> data;
    private int errCode;

    public boolean isResult() {
        return result;
    }

    public List<ConfigItem> getData() {
        return data;
    }

    public int getErrCode() {
        return errCode;
    }

    public static class ConfigItem {

        private int cluster_id;
        private String bid;
        private String topic;
        private String m;

        public int getClusterId() {
            return cluster_id;
        }

        public String getBid() {
            return bid;
        }

        public String getTopic() {
            return topic;
        }

        public String getM() {
            return m;
        }
    }
}
