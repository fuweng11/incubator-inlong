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

package org.apache.inlong.agent.plugin.sources.reader.dbsync;

import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

import java.util.ArrayList;

public class PkgEvent {

    private ArrayList<LogEvent> eventLists;
    private long index;
    private boolean bHasTransEnd = false;

    public PkgEvent(ArrayList<LogEvent> events, long index, boolean bHasTransEnd) {
        this.eventLists = events;
        this.index = index;
        this.bHasTransEnd = bHasTransEnd;
    }

    public ArrayList<LogEvent> getEventLists() {
        return eventLists;
    }

    public void setEventLists(ArrayList<LogEvent> eventLists) {
        this.eventLists = eventLists;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public boolean isbHasTransEnd() {
        return bHasTransEnd;
    }

    public void setbHasTransEnd(boolean bHasTransEnd) {
        this.bHasTransEnd = bHasTransEnd;
    }
}
