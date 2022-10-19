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

import java.io.IOException;

/**
 * Top Abstraction for network packet.<br>
 * it exposes 2 behaviors for sub-class implementation which will be used to
 * marshal data into bytes before sending and to un-marshal data from data after
 * receiving.<br>
 */
public interface IPacket {

    /**
     * un-marshal raw bytes into {@link IPacket} state for application usage.<br>
     *
     * @param data the raw byte data received from networking
     */
    void fromBytes(byte[] data) throws IOException;

    /**
     * marshal the {@link IPacket} state into raw bytes for sending out to
     * network.<br>
     *
     * @return the bytes that's collected from {@link IPacket} state
     */
    byte[] toBytes() throws IOException;
}
