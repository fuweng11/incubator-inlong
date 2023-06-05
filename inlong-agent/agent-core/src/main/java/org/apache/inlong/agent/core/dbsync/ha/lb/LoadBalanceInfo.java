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

package org.apache.inlong.agent.core.dbsync.ha.lb;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LoadBalanceInfo
        implements
            Comparable<LoadBalanceInfo> {

    private static final int PERCENT_HUNDRED = 100;

    private static final float CAPACITY_PERCENT_THRESHOLD = 1.0F;

    public static float CHECK_LOAD_THRESHOLD = 0.6F;

    public static int COMPARE_LOAD_THRESHOLD = 10;

    private int maxDbJobsThreshold;

    /*
     * sync num
     */
    private int dbJobIdNum = 0;

    /*
     * cpc using rate
     */
    private ResourceUsage cpu;

    /*
     * mem using rate
     */
    private ResourceUsage memory;

    private ResourceUsage systemMemory;

    /*
     * bandwidth using rate
     */
    private ResourceUsage bandwidthIn;

    private ResourceUsage bandwidthOut;

    private long timeStamp;

    private String ip;

    /*
     * Priority is given to comparing the capacity utilization rate, and secondly, the resource load is compared within
     * a certain range of the difference in the capacity utilization rate.
     */
    public int compareTo(LoadBalanceInfo o) {

        /*
         * Compare the remaining capacity, Larger remaining capacity is preferred
         */
        float localCapacityPercentUsage = getSyncsCapacityPercentUsage();
        float otherCapacityPercentUsage = o.getSyncsCapacityPercentUsage();

        if (localCapacityPercentUsage < CAPACITY_PERCENT_THRESHOLD
                && otherCapacityPercentUsage == CAPACITY_PERCENT_THRESHOLD) {
            /*
             * when local capacity usage is lower than 100/% and other capacity is equal than 100/% ,use local
             */
            return -1;
        } else if (otherCapacityPercentUsage < CAPACITY_PERCENT_THRESHOLD
                && localCapacityPercentUsage == CAPACITY_PERCENT_THRESHOLD) {
            /*
             * when other capacity usage is lower than 1.0 and local capacity is equal than 100/% ,use other
             */
            return 1;
        } else if (localCapacityPercentUsage == CAPACITY_PERCENT_THRESHOLD
                && otherCapacityPercentUsage == CAPACITY_PERCENT_THRESHOLD) {
            /*
             * No need to compare other parameters anymore
             */
            return 0;
        }

        int capacityPercentUsageComp = (int) (localCapacityPercentUsage * PERCENT_HUNDRED
                - otherCapacityPercentUsage * PERCENT_HUNDRED);
        /*
         * Compare payload if The value of subtracting two capacity percent usage is lower than CHECK_LOAD_THRESHOLD
         */
        int comp = 0;
        if (Math.abs(capacityPercentUsageComp) < COMPARE_LOAD_THRESHOLD) {
            if ((bandwidthIn == null || bandwidthIn.percentUsage() < CHECK_LOAD_THRESHOLD) && (bandwidthOut == null
                    || bandwidthOut.percentUsage() < CHECK_LOAD_THRESHOLD)
                    && (o.getBandwidthIn() == null
                            || o.getBandwidthIn().percentUsage() < CHECK_LOAD_THRESHOLD)
                    && (o.getBandwidthOut() == null
                            || o.getBandwidthOut().percentUsage() < CHECK_LOAD_THRESHOLD)) {
                if ((memory == null || memory.percentUsage() < CHECK_LOAD_THRESHOLD) && (o.getMemory() == null
                        || o.getMemory().percentUsage() < CHECK_LOAD_THRESHOLD)) {
                    if (cpu != null && o.getCpu() != null) {
                        comp = (int) (cpu.percentUsage() * PERCENT_HUNDRED
                                - o.getCpu().percentUsage() * PERCENT_HUNDRED);
                    }
                } else if (memory != null && o.getMemory() != null) {
                    comp = (int) (memory.percentUsage() * PERCENT_HUNDRED
                            - o.getMemory().percentUsage() * PERCENT_HUNDRED);
                }
            } else if (bandwidthIn != null && o.getBandwidthIn() != null) {
                comp = (int) (bandwidthIn.percentUsage() * PERCENT_HUNDRED
                        - o.getBandwidthIn().percentUsage() * PERCENT_HUNDRED);
            }
        }

        /*
         * last compare ip
         */
        if (comp == 0) {
            if (capacityPercentUsageComp != 0) {
                comp = capacityPercentUsageComp;
            }
        }

        if (comp == 0 && ip != null) {
            comp = ip.compareTo(o.getIp());
        }
        return comp;
    }

    public float getSyncsCapacityPercentUsage() {
        float proportion = 0;
        if (maxDbJobsThreshold > 0) {
            proportion = ((float) dbJobIdNum) / ((float) maxDbJobsThreshold);
        }
        return proportion;
    }
}
