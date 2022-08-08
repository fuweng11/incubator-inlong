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

package org.apache.inlong.manager.service.sink.tencent.us;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveSinkDTO;
import org.apache.inlong.manager.pojo.tencent.us.CreateUsTaskRequest;
import org.apache.inlong.manager.pojo.tencent.us.CreateUsTaskRequest.TaskExt;
import org.apache.inlong.manager.pojo.tencent.us.USConfiguration;
import org.apache.inlong.manager.pojo.tencent.us.UpdateUsTaskRequest;
import org.apache.inlong.manager.pojo.tencent.us.UsLinkRequest;
import org.apache.inlong.manager.service.core.BgInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * operate US tasks
 */
@Service
@Slf4j
public class UsTaskService {

    // task of closing partition at normal level
    private static final String US_CLOSE_PARTITION_TYPE = "134";

    // tasks of de duplication reconciliation and closing partitions
    private static final String US_DISTINCT_CLOSE_PARTITION_TYPE = "230";

    // the self dependency type is parallel, that is,
    // the operation of a single task does not depend on the success of the previous instance
    private static final int SELF_DEPEND = 3;

    // thread safe
    private static final Gson GSON = new GsonBuilder().create();

    // Us parameter name
    private static final String BID = "bid";
    private static final String TID = "tid";
    private static final String INTERFACE_NAME = "interface_name";
    private static final String HIVE_SERVER = "hiveServer";
    private static final String TDW_GROUP_NAME = "tdwGroupName";
    private static final String HIVE_DB = "hiveDb";
    private static final String HIVE_TB = "hiveTb";

    private static final String SECURITY_URL = "securityUrl";
    private static final String SUPER_SQL_URL = "superSqlUrl";
    private static final String SERVICE_NAME = "serviceName";
    private static final String PROXY_USER = "proxyUser";
    private static final String CMK = "cmk";
    private static final String JDBC_DRIVER = "jdbcDriver";
    private static final String JDBC_URL = "jdbcUrl";
    private static final String JDBC_USERNAME = "jdbcUsername";
    private static final String JDBC_PASSWORD = "jdbcPassword";
    private static final String QUERY_SQL = "querySql";
    private static final String BIN_LOG_GAP = "binLogGap";
    // delay time after partition end time
    private static final String PART_END_DELAY_TIME = "partEndDelayMinute";

    @Value("${inlong.us.taskTryLimit:5}")
    private int taskTryLimit; // number of retries of the task
    @Value("${inlong.us.taskDelayMinute:15}")
    private int taskDelayMinute; // delayed scheduling minutes of tasks

    @Autowired
    private USConfiguration usConfig;
    @Autowired
    private BgInfoService bgInfoService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;

    /**
     * create us task
     */
    public String createUsTask(CreateUsTaskRequest createRequest) {
        MultiValueMap<String, String> params = this.convert(createRequest);
        String result = sendPostRequest(usConfig.getApiUrl() + "/LhotseTask", params);
        log.info("create us task result={}, param={}", result, params);
        return result;
    }

    /**
     * freeze US tasks and use put requests. The request parameters,
     * request methods, and response parameters are different from others:
     * https://iwiki.woa.com/pages/viewpage.action?pageId=871785594
     */
    public String freezeUsTask(String taskIds, String inCharges) {
        String url = usConfig.getApiUrl() + "/task/freeze";
        List<String> params = Collections.singletonList(taskIds);

        HttpHeaders headers = new HttpHeaders();
        headers.add("loginUser", inCharges);
        String rsp = HttpUtils.putRequest(restTemplate, url, params, headers, new ParameterizedTypeReference<String>() {
        });

        JsonObject rspObj = GSON.fromJson(rsp, JsonObject.class);
        log.info("freeze us task by url {}, header {}, response {}", url, headers, rspObj.toString());
        return rspObj.get("success").getAsString();
    }

    /**
     * unfreeze US task: query whether it is frozen before unfreezing
     * use put request
     * TODO unfreeze when the task is frozen: https://iwiki.woa.com/pages/viewpage.action?pageId=195788429
     */
    public String unfreezeUsTask(String taskId, String inCharges) {
        String url = usConfig.getApiUrl() + "/task/unfreeze";

        List<String> params = Collections.singletonList(taskId);
        HttpHeaders headers = new HttpHeaders();
        headers.add("loginUser", inCharges);
        String rsp = HttpUtils.putRequest(restTemplate, url, params, headers, new ParameterizedTypeReference<String>() {
        });
        JsonObject rspObj = GSON.fromJson(rsp, JsonObject.class);
        log.info("unfreeze us task by url {}, header {}, response {}", url, headers, rspObj);

        return rspObj.get("success").getAsString();
    }

    /**
     * modify US task
     */
    public String updateUsTask(UpdateUsTaskRequest updateUsTaskRequest) {
        MultiValueMap<String, String> params = this.convert(updateUsTaskRequest);
        String result = sendPostRequest(usConfig.getApiUrl() + "/LhotseUpdate", params);
        log.info("update us task result={}, param is={}", result, params);
        return result;
    }

    /**
     * add a dependency for the specified task
     */
    public String addOrUpdateLink(UsLinkRequest linkRequest) {
        MultiValueMap<String, String> params = this.convert(linkRequest);
        String result = sendPostRequest(usConfig.getApiUrl() + "/AddOrUpdateLink", params);
        log.info("add or update us link result={}, param={}", result, params);
        return result;
    }

    private String sendPostRequest(String url, MultiValueMap<String, String> params) {
        log.info("send request to {}, param {}", url, params);

        HttpHeaders headers = new HttpHeaders();
        String rsp = HttpUtils.postRequest(restTemplate, url, params, headers,
                new ParameterizedTypeReference<String>() {
                });
        JsonArray rspArr = GSON.fromJson(rsp, JsonArray.class);
        log.info("url {}, response is {}", url, rspArr.toString());

        JsonObject rspObj = rspArr.get(0).getAsJsonObject();
        if ("success".equals(rspObj.get("state").getAsString())) {
            return rspObj.get("taskId").getAsString();
        } else {
            throw new WorkflowListenerException("failed to send us task request");
        }
    }

    /**
     * convert Java objects to multivaluemap
     */
    private MultiValueMap<String, String> convert(Object obj) {
        MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        Map<String, String> maps = objectMapper.convertValue(obj, new TypeReference<Map<String, String>>() {
        });
        parameters.setAll(maps);

        return parameters;
    }

    /**
     * create or update us tasks
     */
    public String createOrUpdateTask(InlongGroupInfo inlongGroupInfo, InnerHiveFullInfo tableInfo) {
        // if there are frozen US tasks under the current group id+stream ID, reuse them
        String inlongGroupId = tableInfo.getInlongGroupId();
        String inlongStreamId = tableInfo.getInlongStreamId();
        String partitionStrategy = tableInfo.getPartitionCreationStrategy();

        StreamSinkEntity thiveEntity = sinkEntityMapper.selectDeletedThive(inlongGroupId, inlongStreamId);

        if (thiveEntity != null) {
            InnerHiveSinkDTO thiveDTO = InnerHiveSinkDTO.getFromJson(thiveEntity.getExtParams());
            boolean isThive = thiveDTO.getIsThive() == 1;
            // the partition creation strategy must be the same,
            // otherwise the parent-child relationship of the task will be disordered
            boolean needReuse = isThive && StringUtils.isNotBlank(thiveDTO.getUsTaskId())
                    && partitionStrategy.equals(thiveDTO.getPartitionCreationStrategy());
            if (needReuse) {
                try {
                    StreamSinkEntity entity = sinkEntityMapper.selectByPrimaryKey(tableInfo.getSinkId());
                    InnerHiveSinkDTO dto = InnerHiveSinkDTO.getFromJson(entity.getExtParams());
                    if (StringUtils.isBlank(thiveDTO.getUsTaskId())) {
                        tableInfo.setUsTaskId(thiveDTO.getUsTaskId());
                        dto.setUsTaskId(thiveDTO.getUsTaskId());
                    }
                    if (StringUtils.isBlank(tableInfo.getVerifiedTaskId())
                            && TencentConstants.PART_DISTINCT_VERIFIED.equals(partitionStrategy)) {
                        tableInfo.setVerifiedTaskId(thiveDTO.getVerifiedTaskId());
                        dto.setVerifiedTaskId(thiveDTO.getVerifiedTaskId());
                    }

                    // it must be judged to prevent modification failure caused by empty parameters
                    if (StringUtils.isNotBlank(dto.getUsTaskId())) {
                        entity.setExtParams(objectMapper.writeValueAsString(dto));
                        sinkEntityMapper.updateByPrimaryKeySelective(entity);
                        log.info("found frozen us tasks, reuse them for gourp id={}, stream id={}",
                                inlongGroupId, inlongStreamId);
                    }
                } catch (Exception e) {
                    log.error("parsing json string to sink info failed", e);
                    throw new WorkflowListenerException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
                }
            }
        }

        String result = "";
        // create us tasks only for thive
        if (TencentConstants.HIVE_TYPE == tableInfo.getIsThive()) {
            log.warn("only create us task for thive, the storage of [" + tableInfo.getId() + "] is hive");
            return result;
        }

        // when there are multiple us responsible persons (incharge), use ";" division
        String inCharges;
        if (StringUtils.isNotEmpty(inlongGroupInfo.getInCharges())) {
            inCharges = inlongGroupInfo.getInCharges().replace(",", ";");
        } else {
            inCharges = inlongGroupInfo.getCreator();
        }

        try {
            String usTaskId = this.saveCommonTask(inlongGroupInfo, tableInfo, inCharges);
            // If the partition creation strategy is [data De duplication verification passed],
            // you need to add a de duplication reconciliation task
            if (TencentConstants.PART_DISTINCT_VERIFIED.equals(partitionStrategy)) {
                this.saveVerifiedTask(usTaskId, inlongGroupInfo, tableInfo, inCharges);
            }
        } catch (Exception e) {
            log.error("failed to create us task for {} - {}", inlongGroupId, inlongStreamId, e);
            result = inlongGroupId + "-" + inlongStreamId + ",reason: " + e.getMessage() + ",contact the administrator";
        }

        return result;
    }

    /**
     * create tasks of verifying reconciliation and closing partitions
     */
    private void saveVerifiedTask(String parentTaskId, InlongGroupInfo inlongGroupInfo, InnerHiveFullInfo tableInfo,
            String inCharges) {

        String inlongGroupId = inlongGroupInfo.getInlongGroupId();
        String inlongStreamId = tableInfo.getInlongStreamId();

        // The hiveserver of TenPay uses the global parameters set in supersql
        // and does not use the same set as manager + sort to avoid full links
        // extended parameters of de duplication task: bid, TID, hiveserver, tdwgroupname, hivedb, hivetb
        List<TaskExt> extList = this.getDistinctTaskExtList(tableInfo);
        extList.add(new TaskExt(BID, inlongGroupId));
        extList.add(new TaskExt(TID, inlongStreamId));
        extList.add(new TaskExt(HIVE_SERVER, tableInfo.getHiveAddress()));
        extList.add(new TaskExt(TDW_GROUP_NAME, tableInfo.getAppGroupName()));
        extList.add(new TaskExt(HIVE_DB, tableInfo.getDbName()));
        extList.add(new TaskExt(HIVE_TB, tableInfo.getTableName()));

        // if the task ID does not exist, create a new one
        String taskId = tableInfo.getVerifiedTaskId();
        if (StringUtils.isBlank(taskId)) {
            CreateUsTaskRequest createRequest = this.getCreateTaskRequest(inlongGroupInfo, tableInfo, inCharges,
                    extList);
            createRequest.setTaskType(US_DISTINCT_CLOSE_PARTITION_TYPE);
            createRequest.setTaskAction("Inlong warehousing - de duplication verification - close partition");
            createRequest.setTaskName(
                    inlongGroupId + "-" + inlongStreamId + "-VerifiedPartition-" + System.currentTimeMillis());

            // associated parent task:https://iwiki.woa.com/pages/viewpage.action?pageId=352545625
            String parentTask = "{'" + parentTaskId + "':1}";
            createRequest.setParentTaskId(parentTask);
            createRequest.setSelfDepend(SELF_DEPEND);
            taskId = this.createUsTask(createRequest);

            // write task ID to database
            if (StringUtils.isNotBlank(taskId)) {
                StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(tableInfo.getSinkId());
                try {
                    InnerHiveSinkDTO dto = InnerHiveSinkDTO.getFromJson(sinkEntity.getExtParams());
                    dto.setVerifiedTaskId(taskId);
                    sinkEntity.setExtParams(objectMapper.writeValueAsString(dto));
                    sinkEntityMapper.updateByPrimaryKeySelective(sinkEntity);
                } catch (Exception e) {
                    log.error("parsing json string to sink info failed", e);
                    throw new WorkflowListenerException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
                }
            }
        } else {
            // If the task ID exists, update the existing task
            // the modifier must be in the [US person in charge before modification],
            // otherwise there is no permission to operate - if the task is frozen, unfreeze it first
            this.unfreezeUsTask(taskId, inCharges.split(";")[0]);
            UpdateUsTaskRequest updateRequest = getUpdateTaskRequest(taskId, inlongGroupInfo, inCharges, extList);
            this.updateUsTask(updateRequest);
        }
    }

    /**
     * get the task extension parameters of de duplication reconciliation
     */
    private List<TaskExt> getDistinctTaskExtList(InnerHiveFullInfo hiveInfo) {
        List<TaskExt> extList = new ArrayList<>();
        Map<String, String> superSqlUrlMap = usConfig.getSuperSqlUrlMap();
        // Supersql connection parameters
        extList.add(new TaskExt(SECURITY_URL, usConfig.getSecurityUrl()));
        String superSqlUrl = superSqlUrlMap.get(hiveInfo.getAppGroupName());
        superSqlUrl = StringUtils.isBlank(superSqlUrl) ? usConfig.getDefaultSuperSqlUrl() : superSqlUrl;
        extList.add(new TaskExt(SUPER_SQL_URL, superSqlUrl));
        extList.add(new TaskExt(SERVICE_NAME, usConfig.getServiceName()));
        extList.add(new TaskExt(PROXY_USER, usConfig.getProxyUser()));
        extList.add(new TaskExt(CMK, usConfig.getCmk()));

        // parameters of index table
        extList.add(new TaskExt(JDBC_DRIVER, usConfig.getJdbcDriver()));
        extList.add(new TaskExt(JDBC_URL, usConfig.getJdbcUrl()));
        extList.add(new TaskExt(JDBC_USERNAME, usConfig.getJdbcUsername()));
        extList.add(new TaskExt(JDBC_PASSWORD, usConfig.getJdbcPassword()));
        extList.add(new TaskExt(BIN_LOG_GAP, usConfig.getBinLogGap()));
        extList.add(new TaskExt(PART_END_DELAY_TIME, usConfig.getPartEndDelayMinute()));

        // Thive de duplication query statement
        extList.add(new TaskExt(QUERY_SQL, usConfig.getQuerySql()));

        return extList;
    }

    /**
     * create or modify common tasks for closing partitions
     */
    private String saveCommonTask(InlongGroupInfo inlongGroupInfo, InnerHiveFullInfo tableInfo, String inCharges) {
        String inlongGroupId = inlongGroupInfo.getInlongGroupId();
        String inlongStreamId = tableInfo.getInlongStreamId();

        // extended parameters of task
        List<TaskExt> extList = new ArrayList<>();
        TaskExt extBid = TaskExt.builder().propName(BID).propValue(inlongGroupId).build();
        TaskExt extTid = TaskExt.builder().propName(INTERFACE_NAME).propValue(inlongStreamId).build();
        extList.add(extBid);
        extList.add(extTid);

        // if the task ID does not exist, create a new one
        String usTaskId = tableInfo.getUsTaskId();
        if (StringUtils.isBlank(usTaskId)) {
            CreateUsTaskRequest createRequest = this.getCreateTaskRequest(inlongGroupInfo, tableInfo, inCharges,
                    extList);
            createRequest.setTaskType(US_CLOSE_PARTITION_TYPE);
            createRequest.setTaskAction("Inlong warehousing - close partition");
            createRequest.setTaskName(
                    inlongGroupId + "-" + inlongStreamId + "-ClosePartition-" + System.currentTimeMillis());
            // empty JSON string, otherwise US parsing error
            createRequest.setParentTaskId("{}");
            createRequest.setSelfDepend(SELF_DEPEND);
            usTaskId = this.createUsTask(createRequest);

            if (StringUtils.isNotBlank(usTaskId)) {
                StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(tableInfo.getSinkId());
                try {
                    InnerHiveSinkDTO dto = InnerHiveSinkDTO.getFromJson(sinkEntity.getExtParams());
                    dto.setUsTaskId(usTaskId);
                    sinkEntity.setExtParams(objectMapper.writeValueAsString(dto));
                    sinkEntityMapper.updateByPrimaryKeySelective(sinkEntity);
                } catch (Exception e) {
                    log.error("parsing json string to sink info failed", e);
                    throw new WorkflowListenerException(ErrorCodeEnum.SINK_SAVE_FAILED.getMessage());
                }
            }
        } else {
            // If the task ID exists, update the existing task,
            // and the [close partition] task does not need to be unfrozen
            // this.unfreezeUsTask(usTaskId, inCharges.split(";")[0]);
            UpdateUsTaskRequest updateRequest = getUpdateTaskRequest(usTaskId, inlongGroupInfo, inCharges, extList);
            this.updateUsTask(updateRequest);
        }

        return usTaskId;
    }

    private UpdateUsTaskRequest getUpdateTaskRequest(String taskId, InlongGroupInfo inlongGroupInfo, String inCharges,
            List<TaskExt> taskExtList) {
        int bgId;
        int productId;
        List<InlongGroupExtInfo> extInfos = inlongGroupInfo.getExtList();
        Map<String, String> extInfosMap = extInfos.stream()
                .collect(Collectors.toMap(InlongGroupExtInfo::getKeyName, InlongGroupExtInfo::getKeyValue));
        if (extInfosMap.get("bgId") == null || extInfosMap.get("productId") == null) {
            String errMsg = "bgId or productId for inlong group can not null when create us task";
            throw new WorkflowListenerException(errMsg);
        }
        try {
            bgId = Integer.parseInt(extInfosMap.get("bgId"));
            productId = Integer.parseInt(extInfosMap.get("productId"));
        } catch (Exception e) {
            String errMsg = "bgId or productId for inlong group is error when create us task";
            throw new WorkflowListenerException(errMsg);
        }
        return UpdateUsTaskRequest.builder()
                .taskId(taskId)
                .tryLimit(taskTryLimit)
                .startMin(taskDelayMinute)
                // It must be the creator of the business,
                // otherwise it may not have the permission of the application group
                .modifier(inlongGroupInfo.getCreator())
                .bgId(bgInfoService.get(bgId).getUsBgId())
                .inCharge(inCharges)
                .productId(String.valueOf(productId))
                .tdwAppGroup(extInfosMap.get("appGroupName"))
                .taskExt(GSON.toJson(taskExtList))
                .build();
    }

    private CreateUsTaskRequest getCreateTaskRequest(InlongGroupInfo inlongGroupInfo, InnerHiveFullInfo tableInfo,
            String inCharges, List<TaskExt> taskExtList) {
        String cycleNum = tableInfo.getPartitionInterval().toString();
        String cycleUnit = tableInfo.getPartitionUnit();
        String startDate = getStartDate(new Date(), cycleUnit);

        int bgId;
        int productId;
        List<InlongGroupExtInfo> extInfos = inlongGroupInfo.getExtList();
        Map<String, String> extInfosMap = extInfos.stream()
                .collect(Collectors.toMap(InlongGroupExtInfo::getKeyName, InlongGroupExtInfo::getKeyValue));
        if (extInfosMap.get("bgId") == null || extInfosMap.get("productId") == null) {
            String errMsg = "bgId or productId for inlong group can not null when create us task";
            throw new WorkflowListenerException(errMsg);
        }
        try {
            bgId = Integer.parseInt(extInfosMap.get("bgId"));
            productId = Integer.parseInt(extInfosMap.get("productId"));
        } catch (Exception e) {
            String errMsg = "bgId or productId for inlong group is error when create us task";
            throw new WorkflowListenerException(errMsg);
        }
        return CreateUsTaskRequest.builder()
                .cycleNum(cycleNum)
                .cycleUnit(cycleUnit)
                .startDate(startDate)
                .tryLimit(taskTryLimit)
                .startMin(taskDelayMinute)
                // It must be the creator of the business,
                // otherwise it may not have the permission of the application group
                .creater(inlongGroupInfo.getCreator())
                .inCharge(inCharges)
                .bgId(bgInfoService.get(bgId).getUsBgId())
                .productId(String.valueOf(productId))
                .tdwAppGroup(extInfosMap.get("appGroupName"))
                .taskExt(GSON.toJson(taskExtList))
                .build();
    }

    /**
     * encapsulate the scheduling start time according to the scheduling cycle type
     */
    private String getStartDate(Date date, String cycleUnit) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String startDate;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        // determine the scheduling start time according to the scheduling type
        switch (cycleUnit) {
            case "M": // monthly task, which is the first day of the month, is 00:00:00
                calendar.set(Calendar.DAY_OF_MONTH, 1); // the date is set to the first day of the month
                startDate = sdf.format(calendar.getTime()) + " 00:00:00";
                break;
            case "W": // the weekly task is this Monday, and the hours, minutes and seconds are 00:00:00
                // Sunday It's the first day of this week. Finally, it will be Monday next week.
                // It's calculated that one day will be deducted this week
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
                if (1 == dayOfWeek) {
                    calendar.add(Calendar.DAY_OF_MONTH, -1);
                }
                // Adjustment date: current date - (date is the day of the week - calendar.monday)
                // relative to calendar Offset days of Monday
                calendar.add(Calendar.DATE, Calendar.MONDAY - calendar.get(Calendar.DAY_OF_WEEK));

                startDate = sdf.format(calendar.getTime()) + " 00:00:00";
                break;
            default: // for other tasks, hours, minutes and seconds are set to 00:00:00
                // the hours, minutes and seconds of day and hour tasks are 00:00:00
                startDate = sdf.format(date) + " 00:00:00";
                break;
        }

        return startDate;
    }

}
