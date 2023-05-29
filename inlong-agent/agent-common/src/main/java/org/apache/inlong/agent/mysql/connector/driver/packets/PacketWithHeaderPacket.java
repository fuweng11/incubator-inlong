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

package org.apache.inlong.agent.mysql.connector.driver.packets;

import org.apache.inlong.agent.mysql.utils.CanalToStringStyle;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.ToStringBuilder;

public abstract class PacketWithHeaderPacket implements IPacket {

    protected HeaderPacket header;

    protected PacketWithHeaderPacket() {
    }

    protected PacketWithHeaderPacket(HeaderPacket header) {
        setHeader(header);
    }

    public HeaderPacket getHeader() {
        return header;
    }

    public void setHeader(HeaderPacket header) {
        Preconditions.checkNotNull(header);
        this.header = header;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
