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

package org.apache.inlong.agent.mysql.connector.driver.packets.server;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ResultSetPacket {

    private SocketAddress     sourceAddress;
    private List<FieldPacket> fieldDescriptors = new ArrayList<FieldPacket>();
    private List<String>      fieldValues      = new ArrayList<String>();

    public void setFieldDescriptors(List<FieldPacket> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    public List<FieldPacket> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldValues(List<String> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public void setSourceAddress(SocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public SocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public String toString() {
        return "ResultSetPacket [fieldDescriptors=" + fieldDescriptors + ", fieldValues=" + fieldValues
               + ", sourceAddress=" + sourceAddress + "]";
    }

}
