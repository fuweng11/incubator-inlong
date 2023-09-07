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

package org.apache.inlong.manager.service.resource.sort.tencent.iceberg;

import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.IcebergTableCreateRequest;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.IcebergTableCreateRequest.FieldsBean;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.InnerIcebergSink;
import org.apache.inlong.manager.pojo.sink.tencent.iceberg.QueryIcebergTableResponse;
import org.apache.inlong.manager.service.resource.sc.ScService;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.util.JsonUtils.OBJECT_MAPPER;

/**
 * Implementation of sort Iceberg operator
 */
@Service
@Slf4j
public class IcebergBaseOptService {

    private static final int MAX_RETRY_TIMES = 20;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    @Value("${dla.api.url}")
    private String dlaApiUrl;

    @Value("#{${dla.api.map:{'tl':'http://11.151.246.185:8080'}}}")
    private Map<String, String> dlaApiMap = new HashMap<>();

    @Value("${dla.auth.user}")
    private String proxyUser;

    @Value("${dla.auth.cmk}")
    private String cmk;

    @Value("#{'${dla.grant.user:wedata}'.split(',')}")
    private List<String> dlaGrantUser;

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private ScService scService;

    public QueryIcebergTableResponse getTableDetail(InnerIcebergSink icebergSink) {
        try {
            return HttpUtils.request(restTemplate,
                    dlaApiMap.getOrDefault(icebergSink.getClusterTag(), "tl") + "/formation/v2/metadata/getTableDetail?dbName=" + icebergSink.getDbName() + "&table="
                            + icebergSink.getTableName(),
                    HttpMethod.GET, null, getTauthHeader(icebergSink.getCreator()), QueryIcebergTableResponse.class);
        } catch (Exception e) {
            String errMsg = String.format("query iceberg table error for database=%s, table=%s",
                    icebergSink.getDbName(), icebergSink.getTableName());
            log.error(errMsg, e);
            throw new WorkflowException(errMsg);
        }
    }

    public void createTable(InnerIcebergSink icebergSink) throws Exception {
        if (icebergSink == null) {
            log.error("iceberg config is null");
            return;
        }

        IcebergTableCreateRequest request = this.buildRequest(icebergSink);
        log.info("try to create new iceberg table, request: {}", OBJECT_MAPPER.writeValueAsString(request));

        try {
            if (dlaGrantUser.contains(icebergSink.getCreator())) {
                grantPrivilegeBySc(icebergSink, icebergSink.getCreator(), "all", true, false);
            }
            QueryIcebergTableResponse queryRsp = this.getTableDetail(icebergSink);
            log.info("get iceberg table detail result={}", queryRsp);
            // the table exists, and the flow direction status is modified to [Configuration Succeeded].
            // return directly
            String url = dlaApiMap.getOrDefault(icebergSink.getClusterTag(), "tl") + "/formation/v2/metadata/createTable";
            if (queryRsp != null && queryRsp.getCode() != 20005) {
                log.warn("iceberg table [{}.{}] already exists", request.getDb(), request.getTable());
                url = dlaApiMap.getOrDefault(icebergSink.getClusterTag(), "tl") + "/formation/v2/metadata/updateTableSchema";
            }
            // the table does not exist. Create a new table
            String rsp = HttpUtils.postRequest(restTemplate, url,
                    request, getTauthHeader(icebergSink.getCreator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("create iceberg result rsp={}", rsp);
            QueryIcebergTableResponse response = JsonUtils.parseObject(rsp, QueryIcebergTableResponse.class);
            if (!Objects.isNull(response) && response.getCode() == 0) {
                log.info("create iceberg table [{}.{}] success, rsp {}", request.getDb(), request.getTable(), rsp);
                sinkService.updateStatus(icebergSink.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(),
                        response.getMessage());
            } else {
                String errMsg = String.format(
                        "create iceberg table failed in dla, please contact administrator, err=%s", response);
                log.error(errMsg);
                sinkService.updateStatus(icebergSink.getId(), SinkStatus.CONFIG_FAILED.getCode(),
                        errMsg);
                throw new WorkflowException("failed to create iceberg table for " + errMsg);
            }
        } catch (Exception e) {
            String errMsg = String.format("create iceberg table failed for database=%s, table=%s", request.getDb(),
                    request.getTable());
            log.error(errMsg, e);
            // modify the flow direction status to [Configuration Failed]
            sinkService.updateStatus(icebergSink.getId(), SinkStatus.CONFIG_FAILED.getCode(),
                    errMsg);
            throw new WorkflowException(errMsg);
        }
    }

    private IcebergTableCreateRequest buildRequest(InnerIcebergSink icebergSink) {
        IcebergTableCreateRequest createRequest = new IcebergTableCreateRequest();
        createRequest.setDb(icebergSink.getDbName());
        createRequest.setDescription(icebergSink.getDescription());
        createRequest.setTable(icebergSink.getTableName());
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(icebergSink.getId());
        List<FieldsBean> fields = fieldList.stream().map(f -> {
            IcebergTableCreateRequest.FieldsBean field = new IcebergTableCreateRequest.FieldsBean();
            field.setName(f.getFieldName());
            field.setType(f.getFieldType());
            field.setDesc(f.getFieldComment());
            return field;
        }).collect(Collectors.toList());
        createRequest.setFields(fields);
        HashMap<String, Object> map = new HashMap<>();
        if (Objects.equals("APPEND", icebergSink.getAppendMode())) {
            map.put("write.upsert.enabled", false);
        } else {
            map.put("write.upsert.enabled", true);
        }
        map.put("format-version", 2);
        String primaryKey = icebergSink.getPrimaryKey();
        if (StringUtils.isNotBlank(primaryKey)) {
            map.put("primary-key", primaryKey);
        }
        createRequest.setProperties(map);
        return createRequest;
    }

    private HttpHeaders getTauthHeader(String originUser) {
        log.info("begin get tauth header for user: " + originUser);
        HttpHeaders httpHeaders = new HttpHeaders();
        TauthClient client = new TauthClient(proxyUser, cmk);
        String encodedAuthentication = null;
        try {
            encodedAuthentication = client.getAuthentication("dla-openapi", originUser);
        } catch (SecureException e) {
            log.error("generate tauth header error ", e);
        }
        httpHeaders.add("secure-authentication", encodedAuthentication);
        return httpHeaders;
    }

    public void grantPrivilegeBySc(InnerIcebergSink icebergSink, String username, String accessType, boolean isAll,
            boolean isAppGroup)
            throws Exception {
        String database = icebergSink.getDbName();
        String tableName = isAll ? "*" : icebergSink.getTableName();
        String clusterTag = StringUtils.isBlank(icebergSink.getClusterTag()) ? "tl" : icebergSink.getClusterTag();
        scService.grant(username, icebergSink.getDbName(), tableName, accessType, "LAKEHOUSE",
                clusterTag, isAppGroup);
        boolean isSuccess = checkAndGrant(icebergSink, username, accessType, isAll, isAppGroup);
        if (!isSuccess) {
            String errMsg = String.format("grant %s permission failed for user=%s, database=%s, table=%s", accessType,
                    username, database, tableName);
            log.info(errMsg);
            sinkService.updateStatus(icebergSink.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
        log.info("success to grant privilege {} for user={}, database={}, table={}", accessType, username, database,
                tableName);
    }

    /**
     * check permission for database or table through security center
     */
    public boolean checkAndGrant(InnerIcebergSink icebergSink, String username, String accessType, boolean isAll,
            boolean isAppGroup)
            throws Exception {
        String dbName = icebergSink.getDbName();
        String tableName = isAll ? "*" : icebergSink.getTableName();
        String clusterTag = StringUtils.isBlank(icebergSink.getClusterTag()) ? "tl" : icebergSink.getClusterTag();
        log.info("check whether the user has permission to {} a table for user={}, database={}", accessType,
                username, dbName);
        boolean hasPermissions = scService.checkPermissions(username, dbName, tableName, accessType, clusterTag,
                isAppGroup);
        AtomicInteger retryTimes = new AtomicInteger(0);
        while (!hasPermissions && retryTimes.get() < MAX_RETRY_TIMES) {
            log.info("check permission with user={}, hasPermission={}, retryTimes={}, maxRetryTimes={}",
                    username, hasPermissions, retryTimes.get(), MAX_RETRY_TIMES);
            // sleep 5 minute
            Thread.sleep(3 * 1000);
            retryTimes.incrementAndGet();
            hasPermissions = scService.checkPermissions(username, dbName, tableName, accessType, clusterTag,
                    isAppGroup);
        }
        return hasPermissions;
    }
}
