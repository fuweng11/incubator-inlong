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

package org.apache.inlong.agent.mysql.connector.binlog.event;

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

/**
 * An Intvar_log_event will be created just before a Query_log_event, if the
 * query uses one of the variables LAST_INSERT_ID or INSERT_ID. Each
 * Intvar_log_event holds the value of one of these variables.
 *
 * Binary Format
 *
 * The Post-Header for this event type is empty. The Body has two components:
 *
 * <table>
 * <caption>Body for Intvar_log_event</caption>
 *
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 *
 * <tr>
 * <td>type</td>
 * <td>1 byte enumeration</td>
 * <td>One byte identifying the type of variable stored. Currently, two
 * identifiers are supported: LAST_INSERT_ID_EVENT==1 and INSERT_ID_EVENT==2.</td>
 * </tr>
 *
 * <tr>
 * <td>value</td>
 * <td>8 byte unsigned integer</td>
 * <td>The value of the variable.</td>
 * </tr>
 *
 * </table>
 *
 * @version 1.0
 */
public final class IntvarLogEvent extends LogEvent {

    /* Intvar event data */
    public static final int I_TYPE_OFFSET = 0;
    public static final int I_VAL_OFFSET = 1;
    // enum Int_event_type
    public static final int INVALID_INT_EVENT = 0;
    public static final int LAST_INSERT_ID_EVENT = 1;
    public static final int INSERT_ID_EVENT = 2;
    /**
     * Fixed data part: Empty
     *
     * <p>
     * Variable data part:
     *
     * <ul>
     * <li>1 byte. A value indicating the variable type: LAST_INSERT_ID_EVENT =
     * 1 or INSERT_ID_EVENT = 2.</li>
     * <li>8 bytes. An unsigned integer indicating the value to be used for the
     * LAST_INSERT_ID() invocation or AUTO_INCREMENT column.</li>
     * </ul>
     *
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    private final long value;
    private final int type;

    public IntvarLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        /* The Post-Header is empty. The Varible Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen
                + descriptionEvent.postHeaderLen[INTVAR_EVENT - 1]
                + I_TYPE_OFFSET);
        type = buffer.getInt8(); // I_TYPE_OFFSET
        value = buffer.getLong64(); // !uint8korr(buf + I_VAL_OFFSET);
    }

    public final int getType() {
        return type;
    }

    public final long getValue() {
        return value;
    }

    public final String getQuery() {
        return "SET INSERT_ID = " + value;
    }
}
