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

import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
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
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.util.JsonUtils.OBJECT_MAPPER;

/**
 * Implementation of sort Iceberg operator
 */
@Service
@Slf4j
public class IcebergBaseOptService {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    @Value("${dla.api.url}")
    private String dlaApiUrl;

    @Value("${dla.auth.user}")
    private String proxyUser;

    @Value("${dla.auth.cmk}")
    private String cmk;

    @Autowired
    private StreamSinkService sinkService;

    public QueryIcebergTableResponse getTableDetail(InnerIcebergSink icebergSink) {
        try {
            return HttpUtils.request(restTemplate,
                    dlaApiUrl + "/formation/v2/metadata/getTableDetail?dbName=" + icebergSink.getDbName() + "&table="
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
            QueryIcebergTableResponse queryRsp = this.getTableDetail(icebergSink);
            log.info("get iceberg table detail result={}", queryRsp);
            // the table exists, and the flow direction status is modified to [Configuration Succeeded].
            // return directly
            if (queryRsp != null && queryRsp.getCode() != 20005) {
                log.warn("iceberg table [{}.{}] already exists", request.getDb(), request.getTable());
                sinkService.updateStatus(icebergSink.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(),
                        queryRsp.getMessage());
                return;
            }

            // the table does not exist. Create a new table
            String rsp = HttpUtils.postRequest(restTemplate, dlaApiUrl + "/formation/v2/metadata/createTable",
                    request, getTauthHeader(icebergSink.getCreator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("create iceberg result rsp={}", rsp);
            QueryIcebergTableResponse response = JsonUtils.parseObject(rsp, QueryIcebergTableResponse.class);
            if (ObjectUtils.isNotEmpty(response) && response.getCode() == 0) {
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
}
