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
 * Base class for ignorable log events. Events deriving from
 * this class can be safely ignored by slaves that cannot
 * recognize them. Newer slaves, will be able to read and
 * handle them. This has been designed to be an open-ended
 * architecture, so adding new derived events shall not harm
 * the old slaves that support ignorable log event mechanism
 * (they will just ignore unrecognized ignorable events).
 *
 * @version 1.0.3
 * @note The only thing that makes an event ignorable is that it has
 *         the LOG_EVENT_IGNORABLE_F flag set.  It is not strictly necessary
 *         that ignorable event types derive from Ignorable_log_event; they may
 *         just as well derive from Log_event and pass LOG_EVENT_IGNORABLE_F as
 *         argument to the Log_event constructor.
 * @since mysql 5.6
 */
public class IgnorableLogEvent extends LogEvent {

    public IgnorableLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);

        // do nothing , just ignore log event
    }

}
