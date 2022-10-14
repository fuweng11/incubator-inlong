package org.apache.inlong.agent.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;
import org.apache.inlong.agent.entites.CommonResponse;
import org.apache.inlong.agent.entites.FieldChangedEntry;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.JsonUtils.JSONArray;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.TDManagerConn;
import org.apache.inlong.sdk.dataproxy.utils.ConcurrentHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class FieldManager {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private LinkedBlockingQueue<FieldChangedEntry> alterQueue;

    private Set<String> currentChangedSet = new ConcurrentHashSet<String>();

    private FieldChangedTDMThread tdmFieldChangedReportThread;

    private volatile boolean running = true;

    private String token;

    private String serviceName;

    private String columnFieldChangeReportUrl;J

    private int maxRetryTimes;

    private int retryIntervals;

    private int maxQueueSize;

    private List mysqlTypeList;

    private DBSyncConf config; //TODO:移除，用AgentConfiguration.getAgentConf()替换

    public FieldManager() {
        this.token = config.getStringVar(ConfVars.TDMANAGER_AUTH_TOKEN);
        this.serviceName = config.getStringVar(ConfVars.TDMANAGER_SERVICE_NAME);
        tdmFieldChangedReportThread = new FieldChangedTDMThread();
        tdmFieldChangedReportThread.setName(tdmFieldChangedReportThread.getClass().getSimpleName());
        tdmFieldChangedReportThread.setUncaughtExceptionHandler((t, e)
                -> logger.error("{} has an uncaught error: ",
                t.getName(), e));
        maxRetryTimes = config.getIntVar(ConfVars.TDMANAGER_FIELD_CHANGED_REPORT_RETRY_MAX_TIMES);
        retryIntervals = config.getIntVar(ConfVars.TDMANAGER_FIELD_CHANGED_REPORT_INTREVALS);
        columnFieldChangeReportUrl = config.getStringVar(ConfVars.TDMANAGER_FIELD_CHANGED_REPORT_URL);
        maxQueueSize = config.getIntVar(ConfVars.FIELD_CHANGED_MAX_MESSAGE_QUEUE_SIZE);
        String mysqlTypeStr = config.getStringVar(ConfVars.TDMANAGER_FIELD_CHANGED_MYSQL_TYPE_LIST);
        if (StringUtils.isNotEmpty(mysqlTypeStr)) {
            mysqlTypeList = Arrays.asList(mysqlTypeStr.split(","));
        }
        alterQueue = new LinkedBlockingQueue(maxQueueSize);
    }

    public void addAlterFieldString(String bid, String taskId, String alterQueryString) {
        if (StringUtils.isEmpty(bid) || StringUtils.isEmpty(taskId) || StringUtils.isEmpty(alterQueryString)) {
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

    public void clearChangedSet(String bid, String taskId, String alterQueryString) {
        String setStr = bid + taskId + alterQueryString;
        currentChangedSet.remove(setStr);
    }

    public void start() {
        tdmFieldChangedReportThread.start();
    }

    public void stop() {
        running = false;
    }

    private class FieldChangedTDMThread extends Thread {
        @Override
        public void run() {
            logger.info("TDManager FieldChanged Thread running!");
            while (running) {
                try {
                    FieldChangedEntry fieldChangedEntry = alterQueue.take();
                    if (fieldChangedEntry.getJsonArray() == null) {
                        JSONArray array = parseQueryString(fieldChangedEntry);
                        if (array != null) {
                            fieldChangedEntry.setJsonArray(array);
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
                    if (fieldChangedEntry.getJsonArray() != null) {
                        boolean result = false;
                        if (isNeedRetryReport(fieldChangedEntry)) {
                            result = reportFiledChanged(fieldChangedEntry.getTaskId(), fieldChangedEntry.getJsonArray());
                            if (!result) {
                                fieldChangedEntry.addSendTimes();
                                fieldChangedEntry.updateLastReportTime(Instant.now().toEpochMilli());
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

        private boolean reportFiledChanged(String taskId, JSONArray jsonArray) throws Exception{
            JSONObject params = new JSONObject();
            params.put("id", taskId);
            params.put("fields",jsonArray);
            String jobConfigString = TDManagerConn.cgi2TDManager(columnFieldChangeReportUrl, params, token, serviceName);
            CommonResponse<Boolean> commonResponse = CommonResponse.fromJson(jobConfigString, Boolean.class);
            if (commonResponse != null) {
                return commonResponse.getData();
            }
            return false;
        }
    }

    /**
     * parse QueryString
     * @param fieldChangedEntry fieldChangedEntry
     * @return JSONArray
     */
    public JSONArray parseQueryString(FieldChangedEntry fieldChangedEntry) {
        String parseStr = fieldChangedEntry.getAlterQueryString();
        JSONArray resultArray = null;
        if (StringUtils.isNotEmpty(parseStr)) {
            parseStr = parseStr.replaceAll("`|'", "");
            String[] multiAlterArr = parseStr.split(",");
            if (multiAlterArr != null && multiAlterArr.length > 0) {
                for (String alterStr : multiAlterArr) {
                    JSONObject jsonObj = parseQuerySingleAlterString(alterStr);
                    if (jsonObj != null) {
                        if (resultArray == null) {
                            resultArray = new JSONArray();
                        }
                        resultArray.add(jsonObj);
                    }
                }
            }
        }
        return resultArray;
    }

    private JSONObject parseQuerySingleAlterString(String alterString) {
        JSONObject jsonObject = null;
        if (StringUtils.isNotEmpty(alterString) && isAddField(alterString.toLowerCase())) {
            String[] multiFiledArr = alterString.split(" ");
            if (multiFiledArr != null && multiFiledArr.length > 0) {
                int index = 0;
                while(index < (multiFiledArr.length - 2)) {
                    if (multiFiledArr[index].equalsIgnoreCase("add")) {
                        jsonObject = new JSONObject();
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
                            jsonObject.put("fieldName", name);
                            jsonObject.put("fieldType", type);
                        }
                        break;
                    }
                    index ++;
                }
            }
        }
        return jsonObject;
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

}
