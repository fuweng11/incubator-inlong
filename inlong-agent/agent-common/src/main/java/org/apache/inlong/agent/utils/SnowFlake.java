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

package org.apache.inlong.agent.utils;

public class SnowFlake {

    // start timestamp
    private static final long START_STMP = 1480166465631L;
    private static final long MILLS_ONE_DAY = 86400000L;

    private long sequenceBit = 9;
    private long machineBit = 13;

    private long maxMachineNum = -1L ^ (-1L << machineBit);
    private long maxSequence = -1L ^ (-1L << sequenceBit);

    private long machineLeft = sequenceBit;
    private long timestmpLeft = sequenceBit + machineBit;

    private long machineId;
    private long sequence = 0L;
    private long lastStmp = -1L;
    private boolean generateForOneDay = false;

    public SnowFlake(long machineId) {
        if (machineId < 0) {
            throw new IllegalArgumentException(
                    machineId + "machineId can't be greater than MAX_MACHINE_NUM or less than 0 MAX_MACHINE_NUM"
                            + maxMachineNum);
        }

        this.machineId = machineId % maxMachineNum;
    }

    public SnowFlake(long machineId, boolean generateForOneDay) {
        this.generateForOneDay = generateForOneDay;
        if (generateForOneDay) {
            machineBit = 27;
            timestmpLeft = sequenceBit + machineBit;
            maxMachineNum = -1L ^ (-1L << machineBit);
        }
        if (machineId < 0) {
            throw new IllegalArgumentException(
                    machineId + "machineId can't be greater than MAX_MACHINE_NUM or less than 0 MAX_MACHINE_NUM"
                            + maxMachineNum);
        }
        this.machineId = machineId % maxMachineNum;
    }

    /**
     * generate nextId
     */
    public synchronized long nextId() {
        long currStmp = getNewstmp();
        if (currStmp < lastStmp && !generateForOneDay) {
            throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
        }

        if (currStmp == lastStmp) {
            sequence = (sequence + 1) & maxSequence;
            if (sequence == 0L) {
                currStmp = getNextMill();
            }
        } else {
            sequence = 0L;
        }

        lastStmp = currStmp;
        long timeValue = (currStmp - START_STMP);
        if (generateForOneDay) {
            timeValue = currStmp;
        }
        return timeValue << timestmpLeft | machineId << machineLeft | sequence;
    }

    private long getNextMill() {
        long mill = getNewstmp();
        while (mill <= lastStmp) {
            mill = getNewstmp();
        }
        return mill;
    }

    private long getNewstmp() {
        if (generateForOneDay) {
            return System.currentTimeMillis() % MILLS_ONE_DAY;
        }
        return System.currentTimeMillis();
    }
}
