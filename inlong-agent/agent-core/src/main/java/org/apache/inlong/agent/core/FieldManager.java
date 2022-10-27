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

package org.apache.inlong.agent.core;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.entites.CommonResponse;
import org.apache.inlong.agent.entites.FieldChangedEntry;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncAddFieldRequest;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncAddFieldRequest.FieldObject;
import org.apache.inlong.sdk.dataproxy.utils.ConcurrentHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_FIELD_CHANGED_REPORT_INTREVALS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_INTREVALS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES;
import static org.apache.inlong.agent.constant.FetcherConstants.DBSYNC_REPORT_ADD_FIELDS;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_DBSYNC_REPORT_ADD_FIELDS;

public class FieldManager {

    private static final FieldManager FIELD_MANAGER = new FieldManager();
    protected final Logger logger = LogManager.getLogger(this.getClass());
    private final AgentConfiguration config = AgentConfiguration.getAgentConf();
    private LinkedBlockingQueue<FieldChangedEntry> alterQueue;
    private Set<String> currentChangedSet = new ConcurrentHashSet<String>();
    private FieldChangedTDMThread tdmFieldChangedReportThread;
    private volatile boolean running = false;
    private String columnFieldChangeReportUrl;
    private int maxRetryTimes;
    private int retryIntervals;
    private int maxQueueSize;
    private List mysqlTypeList;
    private HttpManager httpManager;

    private FieldManager() {
        tdmFieldChangedReportThread = new FieldChangedTDMThread();
        tdmFieldChangedReportThread.setName(tdmFieldChangedReportThread.getClass().getSimpleName());
        tdmFieldChangedReportThread.setUncaughtExceptionHandler((t, e)
                -> logger.error("{} has an uncaught error: ",
                t.getName(), e));
        maxRetryTimes = config.getInt(DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES,
                DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES);
        retryIntervals = config.getInt(DBSYNC_FIELD_CHANGED_REPORT_INTREVALS,
                DEFAULT_DBSYNC_FIELD_CHANGED_REPORT_INTREVALS);
        columnFieldChangeReportUrl = buildFieldReportUrl();
        maxQueueSize = config.getInt(DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE,
                DEFAULT_DBSYNC_FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE);
        String mysqlTypeStr = config.get(DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST,
                DEFAULT_DBSYNC_FIELD_CHANGED_MYSQL_TYPE_LIST);
        if (StringUtils.isNotEmpty(mysqlTypeStr)) {
            mysqlTypeList = Arrays.asList(mysqlTypeStr.split(","));
        }
        alterQueue = new LinkedBlockingQueue(maxQueueSize);
        httpManager = new HttpManager(config);
    }

    public static FieldManager getInstance() {
        return FIELD_MANAGER;
    }

    private String buildFieldReportUrl() {
        return HttpManager.buildBaseUrl() + config.get(DBSYNC_REPORT_ADD_FIELDS, DEFAULT_DBSYNC_REPORT_ADD_FIELDS);
    }

    public void addAlterFieldString(String bid, Integer taskId, String alterQueryString) {
        if (running) {
            if (StringUtils.isEmpty(bid) || ObjectUtils.isNotEmpty(taskId) || StringUtils.isEmpty(alterQueryString)) {
                logger.warn("addAlterFieldString has argument error bid = {}, taskId = {}, "
                        + "alterQueryString = {}", bid, taskId, alterQueryString);
                return;
            }
            String setStr = bid + taskId + alterQueryString;
            if (currentChangedSet.add(setStr)) {
                alterQueue.add(new FieldChangedEntry(bid, taskId, alterQueryString));
                logger.info("addAlterFieldString bid = {}, taskId = {}, alterString = {}", bid, taskId,
                        alterQueryString);
            } else {
                logger.warn("alterQueue is full!, message will be discard addAlterFieldString bid = "
                        + "{}, taskId = {}, alterString = {}", bid, taskId, alterQueryString);
            }
        }
    }

    public void clearChangedSet(String bid, Integer taskId, String alterQueryString) {
        String setStr = bid + taskId + alterQueryString;
        currentChangedSet.remove(setStr);
    }

    public void start() {
        running = true;
        tdmFieldChangedReportThread.start();
    }

    public void stop() {
        running = false;
    }

    /**
     * parse QueryString
     *
     * @param fieldChangedEntry fieldChangedEntry
     * @return JSONArray
     */
    public List<FieldObject> parseQueryString(FieldChangedEntry fieldChangedEntry) {
        String parseStr = fieldChangedEntry.getAlterQueryString();
        List<FieldObject> resultArray = new ArrayList<>();
        if (StringUtils.isNotEmpty(parseStr)) {
            parseStr = parseStr.replaceAll("`|'", "");
            String[] multiAlterArr = parseStr.split(",");
            if (multiAlterArr != null && multiAlterArr.length > 0) {
                for (String alterStr : multiAlterArr) {
                    FieldObject fieldObject = parseQuerySingleAlterString(alterStr);
                    if (fieldObject != null) {
                        resultArray.add(fieldObject);
                    }
                }
            }
        }
        return resultArray;
    }

    private FieldObject parseQuerySingleAlterString(String alterString) {
        FieldObject fieldObject = null;
        if (StringUtils.isNotEmpty(alterString) && isAddField(alterString.toLowerCase())) {
            String[] multiFiledArr = alterString.split(" ");
            if (multiFiledArr != null && multiFiledArr.length > 0) {
                int index = 0;
                while (index < (multiFiledArr.length - 2)) {
                    if (multiFiledArr[index].equalsIgnoreCase("add")) {
                        String name;
                        String type;
                        if ("column".equalsIgnoreCase(multiFiledArr[index + 1])
                                && (index + 3) < multiFiledArr.length) {
                            name = multiFiledArr[index + 2];
                            type = multiFiledArr[index + 3];
                        } else {
                            name = multiFiledArr[index + 1];
                            type = multiFiledArr[index + 2];
                        }
                        if (isFieldType(type.toLowerCase())) {
                            fieldObject = new FieldObject(name, type);
                        }
                        break;
                    }
                    index++;
                }
            }
        }
        return fieldObject;
    }

    private boolean isAddField(String alterString) {
        if (StringUtils.isEmpty(alterString)
                || !alterString.contains("add")
                || alterString.contains("index")
                || alterString.contains("primary")
                || alterString.contains("key")
                || alterString.contains("index")
                || alterString.contains("unique")) {
            return false;
        }
        return true;
    }

    private boolean isFieldType(String typeStr) {
        if (StringUtils.isEmpty(typeStr)) {
            return false;
        }
        if (typeStr.contains("(")) {
            typeStr = typeStr.substring(0, typeStr.indexOf("("));
        }
        if (mysqlTypeList.contains(typeStr)) {
            return true;
        }
        return false;
    }

    private class FieldChangedTDMThread extends Thread {

        @Override
        public void run() {
            logger.info("TDManager FieldChanged Thread running!");
            while (running) {
                try {
                    FieldChangedEntry fieldChangedEntry = alterQueue.take();
                    if (fieldChangedEntry.getFieldChangedList() == null) {
                        List<FieldObject> array = parseQueryString(fieldChangedEntry);
                        if (CollectionUtils.isNotEmpty(array)) {
                            fieldChangedEntry.setFieldChangedList(array);
                        } else {
                            logger.warn("FieldManager fieldChanged parse has error! and entry will "
                                            + "be discard! bid = {}, tid = {}, alterString = {}",
                                    fieldChangedEntry.getGroupId(),
                                    fieldChangedEntry.getTaskId(),
                                    fieldChangedEntry.getAlterQueryString());
                        }
                    }
                    if (fieldChangedEntry.getSendTimes() > maxRetryTimes) {
                        logger.warn("FieldManager fieldChanged discard changed entry! {}/{}/{}",
                                fieldChangedEntry.getGroupId(), fieldChangedEntry.getTaskId(),
                                fieldChangedEntry.getAlterQueryString());
                        clearChangedSet(fieldChangedEntry.getGroupId(),
                                fieldChangedEntry.getTaskId(), fieldChangedEntry.getAlterQueryString());
                        continue;
                    }
                    if (CollectionUtils.isNotEmpty(fieldChangedEntry.getFieldChangedList())) {
                        boolean result = false;
                        if (isNeedRetryReport(fieldChangedEntry)) {
                            result = reportFiledChanged(fieldChangedEntry);
                            if (!result) {
                                fieldChangedEntry.addSendTimes();
                                fieldChangedEntry.setLastReportTime(Instant.now().toEpochMilli());
                                retryReport(fieldChangedEntry);
                            } else {
                                clearChangedSet(fieldChangedEntry.getGroupId(),
                                        fieldChangedEntry.getTaskId(), fieldChangedEntry.getAlterQueryString());
                            }
                        } else {
                            retryReport(fieldChangedEntry);
                        }
                    }
                } catch (Throwable t) {
                    logger.error("FieldManager FieldChanged getException : {}",
                            DBSyncUtils.getExceptionStack(t));
                }
            }
            logger.info("FieldManager FieldChanged Thread stopped!");
        }

        private void retryReport(FieldChangedEntry fieldChangedEntry) {
            if (!alterQueue.offer(fieldChangedEntry)) {
                clearChangedSet(fieldChangedEntry.getGroupId(), fieldChangedEntry.getTaskId(),
                        fieldChangedEntry.getAlterQueryString());
                logger.warn("alterQueue is full!, message will be discard! addAlterFieldString "
                                + "bid = {}, taskId = {}, alterString = {}", fieldChangedEntry.getGroupId(),
                        fieldChangedEntry.getTaskId(), fieldChangedEntry.getAlterQueryString());
            }
        }

        private boolean isNeedRetryReport(FieldChangedEntry fieldChangedEntry) {
            long now = Instant.now().toEpochMilli();
            if (now - fieldChangedEntry.getLastReportTime() > retryIntervals) {
                return true;
            }
            return false;
        }

        private boolean reportFiledChanged(FieldChangedEntry entry) {
            DbSyncAddFieldRequest request = new DbSyncAddFieldRequest(entry.getTaskId(), entry.getFieldChangedList());
            String jobConfigString = httpManager.doSentPost(columnFieldChangeReportUrl, request);
            CommonResponse<Boolean> commonResponse = CommonResponse.fromJson(jobConfigString, Boolean.class);
            if (commonResponse != null) {
                return commonResponse.getData();
            }
            return false;
        }
    }

}
