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

package org.apache.inlong.agent.conf;

import lombok.Data;

@Data
public class DbAddConfigInfo {

    /*
     * 为了支持域名方式，可以是ip 也可以是域名
     */
    private String dbAddress;

    private String realRemoteIp;

    private Integer port;

    public DbAddConfigInfo(String dbAddress, Integer port) {
        this.dbAddress = dbAddress;
        this.port = port;
        this.realRemoteIp = DBSyncJobConf.getIpByHostname(dbAddress);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof DbAddConfigInfo) {
            if (dbAddress != null
                    && dbAddress.equals(((DbAddConfigInfo) obj).getDbAddress())
                    && port.equals(((DbAddConfigInfo) obj).getPort())) {
                return true;
            }
        }
        return false;
    }

    public String updateRemoteIp() {
        this.realRemoteIp = DBSyncJobConf.getIpByHostname(dbAddress);
        return realRemoteIp;
    }

    public String updateRemoteIp(String remoteIp) {
        this.realRemoteIp = remoteIp;
        return realRemoteIp;
    }

    @Override
    public String toString() {
        return "dbAddress=" + dbAddress + ",port=" + port + ",realRemoteIp=" + realRemoteIp;
    }
}
