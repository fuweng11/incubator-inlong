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

package org.apache.inlong.agent.entites;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.agent.message.BatchProxyMessage;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;

import java.util.ArrayList;
import java.util.HashMap;

public class WaitAckDataInfo {

    private HashMap<String, ArrayList<Pair<LogPosition, Long>>> positionMap;
    private HashMap<String, Integer> instCntMap;
    private LogPosition latestLogPosition;

    public WaitAckDataInfo(BatchProxyMessage pkgData) {
        this.positionMap = new HashMap<>();
        this.instCntMap = new HashMap<>();
    }

    public void updateLogPosition(DBSyncMessage data) {
        positionMap.compute(data.getInstName(), (k, v) -> {
            if (v == null) {
                v = new ArrayList<>();
            }
            v.add(Pair.of(data.getLogPosition(), data.getMsgId()));
            return v;
        });
        latestLogPosition = data.getLogPosition();
        instCntMap.compute(data.getInstName(), (k, v) -> {
            if (v == null) {
                v = 1;
            } else {
                v += 1;
            }
            return v;
        });
    }

    public HashMap<String, ArrayList<Pair<LogPosition, Long>>> getPositionMap() {
        return positionMap;
    }

    public HashMap<String, Integer> getInstCntMap() {
        if (null == instCntMap) {
            return null;
        }

        HashMap<String, Integer> tmpMap = new HashMap<>(instCntMap);
        instCntMap = null;
        return tmpMap;
    }

    public LogPosition getLatestLogPosition() {
        return latestLogPosition;
    }
}
