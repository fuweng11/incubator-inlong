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

package org.apache.inlong.manager.service.resource.sink.tencent.us;

import com.tencent.tdw.ups.client.TdwUps;
import com.tencent.tdw.ups.client.TdwUpsFactory;
import com.tencent.tdw.ups.client.impl.HiveImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.tencent.ups.UPSConfiguration;
import org.apache.inlong.manager.pojo.tencent.ups.UPSCreateTableInfo;
import org.apache.inlong.manager.pojo.tencent.ups.UPSOperateResult;
import org.apache.inlong.manager.pojo.tencent.ups.UpsTableInfo;
import org.apache.inlong.manager.pojo.tencent.ups.UpsTablePrivilege;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * UPS related operations
 */
@Slf4j
@Component
public class UPSOperator {

    @Autowired
    private UPSConfiguration upsConfiguration;

    /**
     * query library table interface
     */
    public UpsTableInfo.TableInfoBean queryTableInfo(int hiveType, String clusterTag, String originalUser,
            String dbName, String tableName) throws Exception {
        log.info("begin to query hive table info by hiveType={}, clusterTag={}, user={}, dbName={}, tableName={}",
                hiveType, clusterTag, originalUser, dbName, tableName);
        TdwUps tdwUps = this.getTdwUps(hiveType, originalUser);
        String queryResult = tdwUps.queryTableInfo(clusterTag, null, null, dbName, tableName);
        log.info("query table info result: {}", queryResult);
        if (StringUtils.isEmpty(queryResult)) {
            throw new Exception("failed to query hive/thive table, the result is null");
        }

        UpsTableInfo resultInfo = JsonUtils.parseObject(queryResult, UpsTableInfo.class);
        switch (resultInfo.getCode()) {
            case "0":
                // return table structure
                return resultInfo.getTableInfo();
            case "70004":
                // table does not exist
                return null;
            case "70002":
                // database does not exist
                throw new Exception("database [" + dbName + "] not exists");
            default:
                throw new Exception(resultInfo.getMessage());
        }
    }

    /**
     * creat table
     */
    public UPSOperateResult createTable(int hiveType, UPSCreateTableInfo tableInfo) throws Exception {
        log.debug("try to create table use ups: {}", JsonUtils.toJsonString(tableInfo));

        try {
            TdwUps tdwUps = this.getTdwUps(hiveType, tableInfo.getUserName());
            String result = tdwUps.createTable(tableInfo.getClusterId(),
                    tableInfo.getUserName(), tableInfo.getPassword(),
                    tableInfo.getDbName(), tableInfo.getTableName(),
                    tableInfo.getCols(), tableInfo.getComment(),
                    tableInfo.getPartType(), tableInfo.getPartKey(),
                    tableInfo.getSubPartType(), tableInfo.getSubPartKey(),
                    tableInfo.getFileFormat(), tableInfo.getCompress(), tableInfo.getFieldsTerminated(),
                    tableInfo.getDataEncoding(), tableInfo.getLocation());

            if (StringUtils.isNotEmpty(result)) {
                log.info("create table from ups, result: {}", result);
                return JsonUtils.parseObject(result, UPSOperateResult.class);
            } else {
                throw new Exception("create table from ups, result is 'null'");
            }
        } catch (Exception e) {
            log.error("create table from ups error", e);
            throw e;
        }
    }

    /**
     * modify table field information
     */
    public UPSOperateResult modifyTableColumn(int hiveType, String clusterTag, String originalUser,
            String dbName, String tableName, String ddls) throws Exception {
        TdwUps tdwUps = this.getTdwUps(hiveType, originalUser);
        String result = tdwUps.modifyTableColumn(clusterTag, null, null, dbName, tableName, ddls);

        log.info("modify table result {}", result);
        if (StringUtils.isNotEmpty(result)) {
            return JsonUtils.parseObject(result, UPSOperateResult.class);
        } else {
            throw new Exception("failed to modify hive table " + result);
        }
    }

    /**
     * determine whether the user is in the application group
     */
    public boolean checkUsersInAppGroup(int hiveType, String originalUser, String appGroupName) {
        String queryResult;
        try {
            TdwUps tdwUps = getTdwUps(hiveType, originalUser);
            queryResult = tdwUps.checkUsersInAppgroup(originalUser, appGroupName);
            log.info("check in app group for user {}, result: {}", originalUser, queryResult);
        } catch (Exception e) {
            log.error("check users in app group from ups error", e);
        }
        return true;
    }

    /**
     * apply for library table permission
     */
    public UPSOperateResult applyTablePrivilege(int hiveType, UpsTablePrivilege privilege) throws Exception {
        log.debug("try to table privilege use ups {}", JsonUtils.toJsonString(privilege));

        String queryResult;
        TdwUps tdwUps = getTdwUps(hiveType, privilege.getUserName());
        queryResult = tdwUps
                .applyTablePriv(privilege.getClusterId(), privilege.getUserName(), privilege.getPassword(),
                        privilege.getDbName(),
                        privilege.getTableName(), privilege.getPrivilege(), privilege.getApplyRange(),
                        privilege.getComment(), privilege.getAccount(), privilege.getAccountID());

        log.info("apply table privilege result {}", queryResult);

        if (StringUtils.isNotEmpty(queryResult)) {
            return JsonUtils.parseObject(queryResult, UPSOperateResult.class);
        } else {
            throw new Exception("fail to apply table privilege " + queryResult);
        }
    }

    private TdwUps getTdwUps(Integer hiveType, String originalUser) throws Exception {
        log.info("begin init ups for hive type={}", hiveType);

        if (hiveType == TencentConstants.HIVE_TYPE) {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            HiveImpl hiveImpl = new HiveImpl();
            hiveImpl.init(upsConfiguration.getHiveServerUrl(), upsConfiguration.getMetaStoreUrl(),
                    upsConfiguration.getSecurityCenterUrl());
            hiveImpl.setProxyUser(originalUser);
            hiveImpl.setUser("tdm");
            hiveImpl.setCMKDir(upsConfiguration.getCmkDir());
            return hiveImpl;
        } else if (hiveType == TencentConstants.THIVE_TYPE) {
            String upsServer = upsConfiguration.getUpsServer();
            if (StringUtils.isBlank(upsServer)) {
                upsServer = "http://tdwopen.oa.com/tdwprivapi";
            }
            TdwUps tdwUps = TdwUpsFactory.getInstance("com.tencent.tdw.ups.client.impl.TdwUpsImpl",
                    upsServer);
            tdwUps.setServer(upsServer);
            tdwUps.setProxyUser(originalUser);
            tdwUps.setUser("tdm");
            tdwUps.setCMKDir(upsConfiguration.getCmkDir());

            return tdwUps;
        } else {
            throw new Exception("hive type " + hiveType + " not support");
        }
    }

}
