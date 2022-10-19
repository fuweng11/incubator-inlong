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

package org.apache.inlong.agent.mysql.connector.driver.utils;

public abstract class MSC {

    public static final int MAX_PACKET_LENGTH = (1 << 24);
    public static final int HEADER_PACKET_LENGTH_FIELD_LENGTH = 3;
    public static final int HEADER_PACKET_LENGTH_FIELD_OFFSET = 0;
    public static final int HEADER_PACKET_LENGTH = 4;
    public static final int HEADER_PACKET_NUMBER_FIELD_LENGTH = 1;

    public static final byte NULL_TERMINATED_STRING_DELIMITER = 0x00;
    public static final byte DEFAULT_PROTOCOL_VERSION = 0x0a;

    public static final int FIELD_COUNT_FIELD_LENGTH = 1;

    public static final int EVENT_TYPE_OFFSET = 4;
    public static final int EVENT_LEN_OFFSET = 9;

    public static final long DEFAULT_BINLOG_FILE_START_POSITION = 4;
}
