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

package org.apache.inlong.agent.core.dbsync;

import java.util.ArrayList;

public class PkgEvent {

    private ArrayList<Object> eventLists;
    private long index;
    private String logFileName;
    private long timeStamp;

    public PkgEvent(ArrayList<Object> events, long index, long timeStamp, String logFileName) {
        this.eventLists = events;
        this.index = index;
        this.timeStamp = timeStamp;
        this.logFileName = logFileName;
    }

    public ArrayList<Object> getEventLists() {
        return eventLists;
    }

    public void setEventLists(ArrayList<Object> eventLists) {
        this.eventLists = eventLists;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
