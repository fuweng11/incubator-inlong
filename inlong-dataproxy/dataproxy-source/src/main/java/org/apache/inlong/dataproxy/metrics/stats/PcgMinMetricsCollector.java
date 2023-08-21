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

package org.apache.inlong.dataproxy.metrics.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Pcg Metrics Collector
 *
 *
 * The pcg metric collector received or sent by DataProxy nodes, and output to file.
 */
public class PcgMinMetricsCollector extends AbsStatsDaemon {

    private static final String RECORD_PREFIX = "simple_source";
    private static final Logger LOGGER = LoggerFactory.getLogger(PcgMinMetricsCollector.class);
    private static final AtomicLong RECODE_ID = new AtomicLong(0);
    private final StatsUnit[] statsUnits = new StatsUnit[2];
    public PcgMinMetricsCollector(String compName, long intervalMill, int maxCnt) {
        super(compName, intervalMill, maxCnt);
        this.statsUnits[0] = new StatsUnit(RECORD_PREFIX);
        this.statsUnits[1] = new StatsUnit(RECORD_PREFIX);
    }

    /**
     * Add success statistic
     *
     * @param key the statistic key
     * @param msgCnt  the message count
     * @param packCnt the package count
     */
    public void addSuccStats(String key, int msgCnt, int packCnt) {
        if (isStopped()) {
            return;
        }
        statsUnits[getWriteIndex()].addSuccCnt(key, msgCnt, packCnt);
    }

    @Override
    protected int loopProcess(long startTime) {
        return statsUnits[getReadIndex()].printAndResetStatsInfo(startTime);
    }

    @Override
    protected int exitProcess(long startTime) {
        int totalCnt = 0;
        if (!statsUnits[getReadIndex()].isEmpty()) {
            totalCnt += statsUnits[getReadIndex()].printAndResetStatsInfo(startTime);
        }
        if (!statsUnits[getWriteIndex()].isEmpty()) {
            totalCnt += statsUnits[getWriteIndex()].printAndResetStatsInfo(startTime);
        }
        return totalCnt;
    }

    private static class StatsUnit {

        private final String statsName;
        private final ConcurrentHashMap<String, StatsItem> counterMap = new ConcurrentHashMap<>();

        public StatsUnit(String statsName) {
            this.statsName = statsName;
        }

        public boolean isEmpty() {
            return counterMap.isEmpty();
        }

        public void addSuccCnt(String key, int msgCnt, int packCnt) {
            StatsItem statsItem = counterMap.get(key);
            if (statsItem == null) {
                StatsItem tmpItem = new StatsItem();
                statsItem = counterMap.putIfAbsent(key, tmpItem);
                if (statsItem == null) {
                    statsItem = tmpItem;
                }
            }
            statsItem.addSuccessCnt(msgCnt, packCnt);
        }

        public int printAndResetStatsInfo(long startTime) {
            int printCnt = 0;
            // get print time (second)
            long printTime = startTime / 1000;
            for (Map.Entry<String, StatsItem> entry : counterMap.entrySet()) {
                if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                LOGGER.info("{}#{}_{}#{}#{}", this.statsName, printTime,
                        RECODE_ID.incrementAndGet(), entry.getKey(), entry.getValue().toString());
                printCnt++;
            }
            counterMap.clear();
            return printCnt;
        }
    }

    private static class StatsItem {

        private final LongAdder msgCnt = new LongAdder();
        private final LongAdder packCnt = new LongAdder();

        public StatsItem() {

        }

        public void addSuccessCnt(int msgCnt, int packCnt) {
            this.msgCnt.add(msgCnt);
            this.packCnt.add(packCnt);
        }

        @Override
        public String toString() {
            return packCnt.longValue() + "#" + msgCnt.longValue();
        }
    }

}
