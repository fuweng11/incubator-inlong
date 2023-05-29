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

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.driver.packets.UUIDSet.Interval;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;

import com.google.common.collect.Lists;
import lombok.Synchronized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.agent.mysql.connector.binlog.event.GtidLogEvent.ENCODED_SID_LENGTH;

public class MysqlGTIDSet implements GTIDSet {

    public Map<String, UUIDSet> sets;

    /**
     * parse as MysqlGTIDSet: 726757ad-4455-11e8-ae04-0242ac110002:1 =>
     * MysqlGTIDSet{ sets: { 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:2}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3:4 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:5}] } }
     * } 726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4},
     * {start:7, stop: 10}] } } }
     * 726757ad-4455-11e8-ae04-0242ac110002:1-3,726757
     * ad-4455-11e8-ae04-0242ac110003:4 => MysqlGTIDSet{ sets: {
     * 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] },
     * 726757ad-4455-11e8-ae04-0242ac110003: UUIDSet{ SID:
     * 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:4, stop:5}] } }
     * }
     *
     * @param gtidData
     * @return
     */
    public static MysqlGTIDSet parse(String gtidData) {
        Map<String, UUIDSet> m;

        if (gtidData == null || gtidData.length() < 1) {
            m = new ConcurrentHashMap<>();
        } else {
            String[] uuidStrs = gtidData.replaceAll("\n", "").split(",");
            m = new HashMap<>(uuidStrs.length);
            for (int i = 0; i < uuidStrs.length; i++) {
                UUIDSet uuidSet = UUIDSet.parse(uuidStrs[i]);
                m.put(uuidSet.sid.toString(), uuidSet);
            }
        }

        MysqlGTIDSet gs = new MysqlGTIDSet();
        gs.sets = m;

        return gs;
    }

    public static MysqlGTIDSet parse(LogBuffer buffer) {
        Map<String, UUIDSet> m = new ConcurrentHashMap<>();

        if (buffer != null && buffer.hasRemaining()) {
            long nsids = buffer.getLong64();
            for (long i = 0; i < nsids; i++) {
                byte[] bytes = buffer.getData(ENCODED_SID_LENGTH);
                ByteBuffer bb = ByteBuffer.wrap(bytes);
                long high = bb.getLong();
                long low = bb.getLong();
                UUID sid = new UUID(high, low);

                long nInternals = buffer.getLong64();

                List<Interval> intervals = Lists.newArrayList();
                for (long n = 0; n < nInternals; n++) {
                    Interval interval = new Interval();
                    interval.start = buffer.getLong64();
                    interval.stop = buffer.getLong64();
                    intervals.add(interval);
                }

                UUIDSet uuidSet = new UUIDSet(sid, intervals);
                m.put(sid.toString(), uuidSet);
            }
        }

        MysqlGTIDSet mysqlGTIDSet = new MysqlGTIDSet();
        mysqlGTIDSet.sets = m;

        return mysqlGTIDSet;
    }

    @Override
    @Synchronized
    public byte[] encode() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedInt64LittleEndian(sets.size(), out);

        for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
            out.write(entry.getValue().encode());
        }

        return out.toByteArray();
    }

    @Override
    @Synchronized
    public void update(String str) {
        UUIDSet us = UUIDSet.parse(str);
        update(us);
    }

    @Synchronized
    public void update(UUIDSet uuidSet) {
        String sid = uuidSet.sid.toString();
        if (sets.containsKey(sid)) {
            sets.get(sid).intervals.addAll(uuidSet.intervals);
            sets.get(sid).intervals = UUIDSet.combine(sets.get(sid).intervals);
        } else {
            sets.put(sid, uuidSet);
        }
    }

    @Synchronized
    public void update(MysqlGTIDSet mysqlGTIDSet) {
        mysqlGTIDSet.sets.values().forEach(this::update);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }

        MysqlGTIDSet gs = (MysqlGTIDSet) o;
        if (gs.sets == null) {
            return false;
        }

        for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
            if (!entry.getValue().equals(gs.sets.get(entry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sets);
    }

    public boolean isEmpty() {
        return sets.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        synchronized (this) {
            for (Map.Entry<String, UUIDSet> entry : sets.entrySet()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(entry.getValue().toString());
            }
            return sb.toString();
        }
    }
}
