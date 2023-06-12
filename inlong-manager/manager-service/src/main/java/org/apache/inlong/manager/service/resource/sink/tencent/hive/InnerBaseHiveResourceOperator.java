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

package org.apache.inlong.manager.service.resource.sink.tencent.hive;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.tencent.InnerBaseHiveDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.hive.HiveColumnInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerBaseHiveSinkDTO;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.tencent.sc.AppGroup;
import org.apache.inlong.manager.pojo.tencent.sc.ResourceGrantRequest;
import org.apache.inlong.manager.pojo.tencent.sc.ResourceGrantRequest.DataAccessType;
import org.apache.inlong.manager.pojo.tencent.sc.ScHiveResource;
import org.apache.inlong.manager.pojo.tencent.ups.DealResult;
import org.apache.inlong.manager.pojo.tencent.ups.UPSCreateTableInfo;
import org.apache.inlong.manager.pojo.tencent.ups.UPSOperateResult;
import org.apache.inlong.manager.pojo.tencent.ups.UpsTableInfo.TableInfoBean;
import org.apache.inlong.manager.pojo.tencent.ups.UpsTableInfo.TableInfoBean.ColsInfoBean;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.node.DataNodeService;
import org.apache.inlong.manager.service.resource.sc.ScAuthorize;
import org.apache.inlong.manager.service.resource.sc.ScService;
import org.apache.inlong.manager.service.resource.sink.SinkResourceOperator;
import org.apache.inlong.manager.service.resource.sink.tencent.us.UPSOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.sink.tencent.us.UsTaskService;
import org.apache.inlong.manager.service.stream.InlongStreamService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class InnerBaseHiveResourceOperator implements SinkResourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerBaseHiveResourceOperator.class);
    private static final Gson GSON = new GsonBuilder().create();
    private static final int MAX_RETRY_TIMES = 20;

    @Autowired
    private ScService scService;
    @Autowired
    private ScAuthorize scAuthorize;
    @Autowired
    private UPSOperator upsOperator;
    @Autowired
    private UsTaskService usTaskService;

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private DataNodeService dataNodeService;
    @Autowired
    private StreamSinkEntityMapper sinkMapper;
    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    public static String zeroFormat(String num, int len, boolean prev) {
        int l = num.length();
        StringBuilder sb = new StringBuilder();
        if (!prev) {
            // after filling
            sb.append(num);
        }
        for (int i = 0; i < len - l; i++) {
            sb.append("0");
        }
        if (prev) {
            // pre filling
            sb.append(num);
        }
        return sb.toString();
    }

    @Override
    public Boolean accept(String sinkType) {
        return Objects.equals(SinkType.INNER_HIVE, sinkType) || Objects.equals(SinkType.INNER_THIVE, sinkType);
    }

    /**
     * create hive table according to the groupId and hive config
     */
    @Override
    public void createSinkResource(SinkInfo sinkInfo) {
        if (sinkInfo == null) {
            LOGGER.warn("sink info was null, skip to create resource");
            return;
        }

        if (SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(sinkInfo.getStatus())) {
            LOGGER.warn("sink resource [" + sinkInfo.getId() + "] already success, skip to create");
            return;
        } else if (InlongConstants.DISABLE_CREATE_RESOURCE.equals(sinkInfo.getEnableCreateResource())) {
            LOGGER.warn("create resource was disabled, skip to create for [" + sinkInfo.getId() + "]");
            return;
        }

        this.createTable(sinkInfo);
    }

    /**
     * create hive/thive table through ups
     */
    private DealResult createTdwTable(InnerHiveFullInfo innerHiveInfo, List<HiveColumnInfo> columnInfoList)
            throws Exception {
        UPSCreateTableInfo tableInfo = this.getTableInfo(innerHiveInfo, columnInfoList);
        UPSOperateResult createResult = upsOperator.createTable(innerHiveInfo.getIsThive(), tableInfo);

        DealResult result = new DealResult();
        if (createResult.getCode().equals("0")) {
            result.setSuccess(true);
            result.setMessage("success to create tdw hive table ");
        } else {
            result.setSuccess(false);
            result.setMessage("failed to create tdw hive table: " + createResult.getMessage());
        }

        return result;
    }

    private void createTable(SinkInfo sinkInfo) {
        LOGGER.info("begin to create hive table for sinkId={}", sinkInfo.getId());

        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(sinkInfo.getId());
        if (CollectionUtils.isEmpty(fieldList)) {
            LOGGER.warn("no hive fields found, skip to create table for sinkId={}", sinkInfo.getId());
        }

        // set columns
        List<HiveColumnInfo> columnList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            HiveColumnInfo columnInfo = new HiveColumnInfo();
            columnInfo.setName(field.getFieldName());
            columnInfo.setType(field.getFieldType());
            columnInfo.setDesc(field.getFieldComment());
            columnList.add(columnInfo);
        }

        String groupId = sinkInfo.getInlongGroupId();
        String streamId = sinkInfo.getInlongStreamId();
        try {
            InnerBaseHiveSinkDTO hiveInfo = InnerBaseHiveSinkDTO.getFromJson(sinkInfo.getExtParams());
            StreamSinkEntity sink = sinkMapper.selectByPrimaryKey(sinkInfo.getId());
            InnerBaseHiveDataNodeInfo dataNodeInfo = (InnerBaseHiveDataNodeInfo) dataNodeService.get(
                    sink.getDataNodeName(), sink.getSinkType());

            // get bg id
            InlongGroupInfo groupInfo = groupService.get(groupId);
            InlongStreamInfo streamInfo = streamService.get(groupId, streamId);
            Integer clusterId = scService.getClusterIdByIdentifier(dataNodeInfo.getClusterTag());
            AppGroup appGroup = scService.getAppGroup(clusterId, groupInfo.getAppGroupName());
            hiveInfo.setBgId(appGroup.getBgId());

            InnerHiveFullInfo hiveFullInfo = InnerBaseHiveSinkDTO.getFullInfo(groupInfo, streamInfo, hiveInfo, sinkInfo,
                    dataNodeInfo);
            String dbName = hiveFullInfo.getDbName();
            String tbName = hiveFullInfo.getTableName();
            String superUser = hiveFullInfo.getUsername();
            if (superUser.startsWith("tdw_")) {
                superUser = superUser.replaceFirst("tdw_", "");
            }
            boolean isExist = upsOperator.checkTableExist(hiveFullInfo, dbName, tbName);
            DealResult dealResult;
            if (isExist) {
                grantPrivilegeBySc(hiveFullInfo, superUser, "select", false, false);
                TableInfoBean existTable = upsOperator.queryTableInfo(hiveFullInfo.getIsThive(),
                        hiveFullInfo.getClusterTag(), hiveFullInfo.getUsername(), dbName, tbName);
                LOGGER.info("hive table [{}.{}] exists, it will be updated", dbName, tbName);
                dealResult = compareTableAndModifyColumn(existTable, hiveFullInfo);
            } else {
                // grant superuser permission to create tables, and create tables with superuser
                grantPrivilegeBySc(hiveFullInfo, superUser, "create", true, false);
                LOGGER.info("new hive table [{}.{}] will be created", dbName, tbName);
                dealResult = this.createTdwTable(hiveFullInfo, columnList);
            }

            // failed to create / modify the table structure. Modify the storage status and business status
            String dealMessage = dealResult.getMessage();
            if (!dealResult.isSuccess()) {
                LOGGER.error("failed to deal tdw hive table, group id={}, stream id={}", groupId, streamId);
                if (dealMessage.length() > 1000) {
                    dealMessage = dealMessage.substring(0, 1000);
                }
                String errMsg = "create hive table failed: " + dealMessage;
                sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
                throw new WorkflowException(errMsg);
            }

            // after the table is created successfully, need to set the table alter and select permission for the super
            // user
            String hiveType = hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE ? "THIVE" : "HIVE";
            grantPrivilegeBySc(hiveFullInfo, groupInfo.getAppGroupName(), "select", false, true);
            grantPrivilegeBySc(hiveFullInfo, sink.getCreator(), "select", false, false);
            scService.grant(superUser, dbName, tbName, "alter", hiveType, hiveFullInfo.getClusterTag(), false);

            // give the responsible person query permission
            // and the write permission is only required by cluster level users of sort side write partition
            InlongGroupInfo inlongGroupInfo = groupService.get(sinkInfo.getInlongGroupId());
            DealResult grantResult = grantPrivilege(inlongGroupInfo.getInCharges().trim(), hiveFullInfo, "select");
            LOGGER.info("grant hive privilege finished: {}", grantResult.getMessage());

            // give the responsible person query permission for default selectors
            String defaultSelectors = hiveFullInfo.getDefaultSelectors();
            if (StringUtils.isNotEmpty(defaultSelectors)) {
                DealResult grantDefaultResult = grantPrivilege(defaultSelectors.trim(), hiveFullInfo, "select");
                String reason = dealMessage + " and " + grantDefaultResult.getMessage();
                LOGGER.info("grant hive privilege finished for default result: {}", reason);
            }
            LOGGER.info("success to deal tdw hive table for group id={}, stream id={}", groupId, streamId);
            // modify sink status
            if (dealMessage.length() > 1000) {
                dealMessage = dealMessage.substring(0, 1000);
            }
            String info = "success to create inner hive/thive resource";
            if (hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE) {
                String result = usTaskService.createOrUpdateTask(inlongGroupInfo, hiveFullInfo);
                if (result != null && result.length() > 0) {
                    throw new WorkflowException("failed to create us task for " + result);
                }
            }
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), dealMessage);
            LOGGER.info(info + " for sinkInfo={}", sinkInfo);
        } catch (Throwable e) {
            String errMsg = "create inner hive/thive table failed: " + e.getMessage();
            LOGGER.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
    }

    /**
     * get the information of UPS creation table
     */
    private UPSCreateTableInfo getTableInfo(InnerHiveFullInfo innerHiveInfo, List<HiveColumnInfo> columnInfoList) {
        UPSCreateTableInfo tableInfo = UPSCreateTableInfo.builder()
                .clusterId(innerHiveInfo.getClusterTag())
                .dbName(innerHiveInfo.getDbName())
                .tableName(innerHiveInfo.getTableName())
                .fileFormat(innerHiveInfo.getFileFormat())
                .partKey(innerHiveInfo.getPrimaryPartition())
                .dataEncoding(innerHiveInfo.getSinkEncoding())
                .build();

        // use the super user used by sort to create the table (configured in the cluster)
        // In the old version, the root user was used to create the table
        // and the authentication was performed through the security center when querying the data
        tableInfo.setUserName(innerHiveInfo.getUsername());

        // when the underlying system creates a table, the note information cannot be empty
        String comment = innerHiveInfo.getDescription();
        if (StringUtils.isBlank(tableInfo.getComment())) {
            comment = "table for" + innerHiveInfo.getInlongStreamId();
        }
        tableInfo.setComment(comment);

        // field separator
        String separator = innerHiveInfo.getTargetSeparator();
        if (StringUtils.isNotEmpty(separator)) {
            try {
                int ascii = Integer.parseInt(separator);
                separator = "\\" + zeroFormat(Integer.toOctalString(ascii), 3, true);
            } catch (NumberFormatException e) {
                separator = null;
                LOGGER.error("failed to format filed separator, use original");
            }
        }
        tableInfo.setFieldsTerminated(separator);

        // encapsulate table fields
        List<Map<String, String>> columnList = new ArrayList<>();
        String priPartition = innerHiveInfo.getPrimaryPartition();
        String secPartition = innerHiveInfo.getSecondaryPartition();
        boolean isThive = innerHiveInfo.getIsThive() == TencentConstants.THIVE_TYPE;
        for (HiveColumnInfo field : columnInfoList) {
            // to remove the partition field from the common column of community hive
            String fieldName = field.getName();
            if (fieldName.equals(priPartition) || fieldName.equals(secPartition)) {
                continue;
            }

            Map<String, String> column = new HashMap<>();
            // when the ups bottom layer operates hive, the field cannot be escaped:
            // [{\"name\":\"`clusterid`\",\"comment\":\"NoComment\",\"type\":\"string\"}]
            // error:Error while compiling statement:
            // FAILED:ParseException cannot recognize input near 'clusterid' '' 'string' in column type
            if (isThive) {
                column.put("name", "`" + fieldName + "`");
            } else {
                column.put("name", fieldName);
            }
            // source field type converted to hive recognizable type
            column.put("type", HiveFieldTypeConvertor.convertFieldType(field.getType()));
            column.put("comment", field.getDesc());
            columnList.add(column);
        }

        // The partition field should be added to the general field of thive: CDG requirements
        // and the partition field should be added to the front
        if (isThive) {
            // set secondary partition
            String partType = innerHiveInfo.getPartitionType();
            tableInfo.setPartKey(priPartition);
            tableInfo.setPartType(partType);

            Map<String, String> column = new HashMap<>();
            column.put("name", priPartition);
            column.put("type", "string");
            column.put("comment", "primary partition");
            columnList.add(0, column); // the first level partition field is at the top

            if (secPartition != null) {
                Map<String, String> secPartColumn = new HashMap<>();
                secPartColumn.put("name", secPartition);
                secPartColumn.put("type", "string");
                secPartColumn.put("comment", "secondary partition");
                columnList.add(1,
                        secPartColumn); // the secondary partition field is placed after the primary partition field

                tableInfo.setSubPartKey(secPartition);
                tableInfo.setSubPartType(partType);
            }

        } else {
            // TODO the partition type of community hive is temporarily defaulted to string
            String partType = "string";
            tableInfo.setPartType(partType);
            if (secPartition != null) {
                tableInfo.setSubPartType(partType);
                tableInfo.setSubPartKey(secPartition);
            }
        }

        tableInfo.setCols(GSON.toJson(columnList));
        return tableInfo;
    }

    private DealResult compareTableAndModifyColumn(TableInfoBean existTable, InnerHiveFullInfo hiveInfo)
            throws Exception {
        DealResult result = new DealResult();
        if (existTable == null) {
            return result;
        }

        // fields configured in the system, remove partition fields
        List<HiveColumnInfo> newFields = getNewFields(hiveInfo);
        // remove the partition field for the existing field in the table
        List<HiveColumnInfo> existFields = getExistFields(existTable);
        LOGGER.info("newFields: {}, existFields: {}", newFields, existFields);

        int newFieldNum = newFields.size();
        int existFieldNum = existFields.size();
        // there are too many fields in the table, so it fails directly
        if (existFieldNum > newFieldNum) {
            throw new Exception("conflict fields, exists fields num: " + existFieldNum
                    + ", new fields num: " + newFieldNum);
        }

        boolean conflict = false;
        int minSize = existFieldNum; // existFieldNum <= newFieldNum
        StringBuilder conflictCol = new StringBuilder();
        // field order, name and type must be consistent
        for (int i = 0; i < minSize; i++) {
            String newName = newFields.get(i).getName();
            String existName = existFields.get(i).getName();
            String newType = HiveFieldTypeConvertor.convertFieldType(newFields.get(i).getType());
            String existType = existFields.get(i).getType();
            boolean same = newName.equalsIgnoreCase(existName) && newType.equalsIgnoreCase(existType);
            if (!same) {
                String existField = existName + "." + existType;
                String newField = newName + "." + newType;
                LOGGER.info("column conflict to hive/thive table: {} vs {}", existField, newField);
                conflictCol.append(existField).append(" vs ").append(newField).append("; ");
                conflict = true;
            }
        }

        // field conflict, direct failure
        if (conflict) {
            String str = conflictCol.substring(0, conflictCol.length() - 2);
            throw new Exception("column conflict to hive/thive table: " + str);
        }

        // add the new field to hive / thive's table
        int colAddIndex = newFieldNum - minSize;
        // same field
        if (colAddIndex == 0) {
            LOGGER.info("table already exists in tdw: {}", hiveInfo.getDbName() + "." + hiveInfo.getTableName());
            result.setSuccess(true);
            result.setMessage("table already exists in tdw");
        } else {
            JSONArray ddlArray = new JSONArray();
            List<HiveColumnInfo> colsToAdd = newFields.subList(minSize, newFieldNum);
            for (HiveColumnInfo col : colsToAdd) {
                JSONObject ddl = new JSONObject();
                ddl.put("op", "ADD");
                if (hiveInfo.getIsThive() == TencentConstants.THIVE_TYPE) {
                    ddl.put("columnNewName", "`" + col.getName() + "`");
                } else {
                    ddl.put("columnNewName", col.getName());
                }
                ddl.put("columnType", col.getType());
                ddl.put("comment", col.getDesc() == null ? " inlong add comment" : col.getDesc());
                ddl.put("order", String.valueOf(minSize + 1));
                ddlArray.put(ddl);
                minSize++;
            }
            LOGGER.info("try to add tdw column {} by user {}", ddlArray, hiveInfo.getUsername());

            UPSOperateResult modifyResult = upsOperator.modifyTableColumn(hiveInfo.getIsThive(),
                    hiveInfo.getClusterTag(), hiveInfo.getUsername(),
                    hiveInfo.getDbName(), hiveInfo.getTableName(),
                    ddlArray.toString());
            if (modifyResult.getCode().equals("0")) { // update success
                result.setSuccess(true);
                result.setMessage("success to modify hive table column ");
            } else {
                result.setSuccess(false);
                result.setMessage("failed to modify hive table " + modifyResult.getMessage());
            }
        }

        return result;
    }

    /**
     * get the table fields configured in the system
     */
    private List<HiveColumnInfo> getNewFields(InnerHiveFullInfo hiveInfo) {
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(hiveInfo.getSinkId());
        if (CollectionUtils.isEmpty(fieldList)) {
            LOGGER.warn("no hive fields found, skip to create table for sinkId={}", hiveInfo.getSinkId());
        }
        String priPartition = hiveInfo.getPrimaryPartition();
        String secPartition = hiveInfo.getSecondaryPartition();
        List<HiveColumnInfo> columnList = new ArrayList<>();
        for (StreamSinkFieldEntity entity : fieldList) {
            String fieldName = entity.getFieldName();
            if (fieldName.equalsIgnoreCase(priPartition) || fieldName.equalsIgnoreCase(secPartition)) {
                continue;
            }
            HiveColumnInfo col = new HiveColumnInfo();
            col.setName(fieldName);
            col.setType(entity.getFieldType());
            col.setDesc(entity.getFieldComment());
            columnList.add(col);
        }

        return columnList;
    }

    /**
     * get fields in hive / thive table
     */
    private List<HiveColumnInfo> getExistFields(TableInfoBean existTable) {
        // normal field after removing partition field
        List<HiveColumnInfo> columnList = new ArrayList<>();
        String priPartition = existTable.getPriPartKey();
        String secPartition = existTable.getSubPartKey();
        for (ColsInfoBean bean : existTable.getColsInfo()) {
            String colName = bean.getColName();
            if (colName.equalsIgnoreCase(priPartition) || colName.equalsIgnoreCase(secPartition)) {
                continue;
            }
            HiveColumnInfo column = new HiveColumnInfo();
            column.setName(colName);
            column.setType(bean.getColType());

            columnList.add(column);
        }
        return columnList;
    }

    public DealResult grantPrivilege(String selectors, InnerHiveFullInfo hiveInfo, String privilegeType) {
        String clusterTag = hiveInfo.getClusterTag();
        String[] accounts = selectors.split(",");
        if (selectors.contains(";")) {
            accounts = selectors.split(";");
        }

        String hiveType = (hiveInfo.getIsThive() == TencentConstants.HIVE_TYPE
                ? ScHiveResource.TYPE_HIVE
                : ScHiveResource.TYPE_THIVE);

        DealResult dealResult = new DealResult();
        try {
            Integer clusterId = scService.getClusterIdByIdentifier(clusterTag);
            Preconditions.expectNotNull(clusterId, "unknown clusterTag: " + clusterTag);

            ScHiveResource scHiveResource = new ScHiveResource();
            scHiveResource.setType(hiveType);
            scHiveResource.setDatabase(hiveInfo.getDbName());
            scHiveResource.setTable(hiveInfo.getTableName());
            scHiveResource.setClusterId(clusterId);
            scHiveResource.setClusterIdentifier(clusterTag);

            ResourceGrantRequest grantRequest = ResourceGrantRequest.builder()
                    .type(ResourceGrantRequest.TYPE_HIVE_USER)
                    .resource(scHiveResource)
                    .users(Arrays.asList(accounts))
                    .dataAccessType(DataAccessType.PLAIN_TEXT)
                    .accesses(Collections.singletonList(privilegeType))
                    .duration(12)
                    .durationUnit(ChronoUnit.MONTHS)
                    .liveMonth(12)
                    .description("grant " + privilegeType + " privilege for InLong")
                    .build();

            scAuthorize.grant(grantRequest);
            dealResult.setSuccess(true);
            dealResult.setMessage("success to apply table privilege for users: " + Arrays.toString(accounts));
        } catch (Exception e) {
            LOGGER.error("grant hive privilege failed", e);
            dealResult.setSuccess(false);
            dealResult.setMessage("failed to apply table privilege for users: " + Arrays.toString(accounts));
        }

        return dealResult;
    }

    /**
     * check permission for database or table through security center
     */
    public boolean checkAndGrant(InnerHiveFullInfo hiveFullInfo, String username, String accessType, boolean isAll,
            boolean isAppGroup)
            throws Exception {
        String dbName = hiveFullInfo.getDbName();
        String tableName = isAll ? "*" : hiveFullInfo.getTableName();
        String clusterTag = hiveFullInfo.getClusterTag();
        LOGGER.info("check whether the user has permission to {} a table for user={}, database={}", accessType,
                username, dbName);
        boolean hasPermissions = scService.checkPermissions(username, dbName, tableName, accessType, clusterTag,
                isAppGroup);
        AtomicInteger retryTimes = new AtomicInteger(0);
        while (!hasPermissions && retryTimes.get() < MAX_RETRY_TIMES) {
            LOGGER.info("check permission with user={}, hasPermission={}, retryTimes={}, maxRetryTimes={}",
                    username, hasPermissions, retryTimes.get(), MAX_RETRY_TIMES);
            // sleep 5 minute
            Thread.sleep(3 * 1000);
            retryTimes.incrementAndGet();
            hasPermissions = scService.checkPermissions(username, dbName, tableName, accessType, clusterTag,
                    isAppGroup);
        }
        return hasPermissions;
    }

    public void grantPrivilegeBySc(InnerHiveFullInfo hiveFullInfo, String username, String accessType, boolean isAll,
            boolean isAppGroup)
            throws Exception {
        String database = hiveFullInfo.getDbName();
        String tableName = isAll ? "*" : hiveFullInfo.getTableName();
        String hiveType = hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE ? "THIVE" : "HIVE";
        scService.grant(username, hiveFullInfo.getDbName(), tableName, accessType, hiveType,
                hiveFullInfo.getClusterTag(), isAppGroup);
        boolean isSuccess = checkAndGrant(hiveFullInfo, username, accessType, isAll, isAppGroup);
        if (!isSuccess) {
            String errMsg = String.format("grant %s permission failed for user=%s, database=%s, table=%s", accessType,
                    username, database, tableName);
            LOGGER.info(errMsg);
            sinkService.updateStatus(hiveFullInfo.getSinkId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
        LOGGER.info("success to grant privilege {} for user={}, database={}, table={}", accessType, username, database,
                tableName);
    }
}
