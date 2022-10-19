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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MonitorLogUtils {

    public static Logger JOB_STAT_LOG = LogManager.getLogger("jobStatMonitor");
    public static Logger JOB_START_POSITION_LOG = LogManager.getLogger("startPositionChangedMonitor");
    public static Logger JOB_EVENT_DISCARD_LOG = LogManager.getLogger("eventDiscardMonitor");

    public static String LOG_SEPARATOR = "|";

    /*
     * job stat
     */
    public static String JOB_STAT_INIT = "init";
    public static String JOB_STAT_START_RUN = "start_run";
    public static String JOB_STAT_DUMP_RUN = "dump_run";
    public static String JOB_STAT_STOP = "stop";
    public static String JOB_STAT_ERROR = "error";
    public static String JOB_STAT_RESET = "resetting";
    public static String JOB_STAT_SWITCH = "switching";
    public static String JOB_STAT_RESET_FINISHED = "resetFinish";
    public static String JOB_STAT_SWITCH_FINISHED = "switchFinish";
    public static String JOB_STAT_FETCH_DATA = "startFetchData";
    public static String JOB_STAT_GET_POSITION = "getStartPosition";
    public static String JOB_STAT_GET_POSITION_FINISHED = "getStartPositionFinish";

    /*
     * job discard type
     */
    public static String EVENT_DISCARD_TABLE_ID_NOT_FOUND = "tableIdNotFound";
    public static String EVENT_DISCARD_EXCEED_RETRY_CNT = "exceedRetryCnt";

    public static void printJobStat(String dbInfo, String stat) {
        JOB_STAT_LOG.info(dbInfo + LOG_SEPARATOR + stat + LOG_SEPARATOR + "");
    }

    public static void printJobStat(String dbInfo, String stat, String errorCode, String attachment) {
        JOB_STAT_LOG.info(dbInfo + LOG_SEPARATOR + stat
                + LOG_SEPARATOR + errorCode + LOG_SEPARATOR + attachment);
    }

    public static void printJobStat(String dbInfo, String stat, String errorCode) {
        JOB_STAT_LOG.info(dbInfo + LOG_SEPARATOR + stat + LOG_SEPARATOR + errorCode);
    }

    public static void printStartPositionWhileMiss(String jobName, String currentDBInfo,
            String startPosition) {
        JOB_START_POSITION_LOG.info(jobName + LOG_SEPARATOR + currentDBInfo
                + LOG_SEPARATOR + startPosition);
    }

    public static void printEventDiscard(String jobName, String discardType, String discardMsg) {
        JOB_EVENT_DISCARD_LOG.info(jobName + LOG_SEPARATOR + discardType + LOG_SEPARATOR
                + discardMsg);
    }
}
