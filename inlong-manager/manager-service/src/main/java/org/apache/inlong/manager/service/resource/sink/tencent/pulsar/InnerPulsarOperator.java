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

package org.apache.inlong.manager.service.resource.sink.tencent.pulsar;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeDTO;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSinkDTO;
import org.apache.inlong.manager.service.node.DataNodeOperateHelper;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class InnerPulsarOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(InnerPulsarOperator.class);
    /**
     * The pulsar manager create topic path
     */
    private final String CREATE_TOPIC_PATH = "/pulsar-manager/openapi/topic/create";

    /**
     * The pulsar manager create pulsar tenant path
     */
    private final String CREATE_TENANT_PATH = "/pulsar-manager/openapi/tenant/create";

    /**
     * The pulsar manager create pulsar namespace path
     */
    private final String CREATE_NAMESPACE_PATH = "/pulsar-manager/openapi/namespace/create";

    private final String REQUEST_HEADER_APP_ID = "app_id";

    private final String REQUEST_HEADER_APP_SECRET = "app_secret";

    @Autowired
    private InnerPulsarResourceConfig innerPulsarResourceConfig;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private DataNodeOperateHelper dataNodeOperateHelper;
    @Autowired
    private StreamSinkService sinkService;

    public void createTopic(SinkInfo sinkInfo) {
        try {
            PulsarDataNodeDTO pulsarDataNodeInfo = getPulsarDataNodeInfo(sinkInfo);
            createPulsarTenant(sinkInfo, pulsarDataNodeInfo);
            createPulsarNameSpace(sinkInfo, pulsarDataNodeInfo);
            Map<String, Object> params = buildCreateTopicParams(sinkInfo, pulsarDataNodeInfo);
            String pulsarManagerUrl = innerPulsarResourceConfig.getPulsarManagerUrl();
            postRequest(sinkInfo, params, pulsarManagerUrl + CREATE_TOPIC_PATH);
            final String info = "success to create Pulsar topic";
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
        } catch (Exception e) {
            String errMsg = "create pulsar topic failed: " + e.getMessage();
            LOGGER.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
    }

    private Map<String, Object> buildCreateTopicParams(SinkInfo sinkInfo, PulsarDataNodeDTO pulsarDataNodeInfo) {
        PulsarSinkDTO pulsarSinkDTO = JsonUtils.parseObject(sinkInfo.getExtParams(), PulsarSinkDTO.class);
        Map<String, Object> params = buildRequestBaseParams(pulsarDataNodeInfo, pulsarSinkDTO);
        params.put("namespace", pulsarSinkDTO.getNamespace());
        params.put("topic", pulsarSinkDTO.getTopic());
        params.put("partitions", pulsarSinkDTO.getPartitionNum());
        params.put("isPersistent", true);
        return params;
    }

    private void createPulsarTenant(SinkInfo sinkInfo, PulsarDataNodeDTO pulsarDataNodeDTO) {
        PulsarSinkDTO pulsarSinkDTO = JsonUtils.parseObject(sinkInfo.getExtParams(), PulsarSinkDTO.class);
        Map<String, Object> params = buildRequestBaseParams(pulsarDataNodeDTO, pulsarSinkDTO);
        String pulsarManagerUrl = innerPulsarResourceConfig.getPulsarManagerUrl();
        postRequest(sinkInfo, params, pulsarManagerUrl + CREATE_TENANT_PATH);
    }

    private void createPulsarNameSpace(SinkInfo sinkInfo, PulsarDataNodeDTO pulsarDataNodeDTO) {
        PulsarSinkDTO pulsarSinkDTO = JsonUtils.parseObject(sinkInfo.getExtParams(), PulsarSinkDTO.class);
        Map<String, Object> params = buildRequestBaseParams(pulsarDataNodeDTO, pulsarSinkDTO);
        params.put("namespace", pulsarSinkDTO.getNamespace());
        String pulsarManagerUrl = innerPulsarResourceConfig.getPulsarManagerUrl();
        postRequest(sinkInfo, params, pulsarManagerUrl + CREATE_NAMESPACE_PATH);
    }

    private void postRequest(SinkInfo sinkInfo, Map<String, Object> params, String url) {
        PulsarManagerResult pulsarManagerResult = HttpUtils.postRequest(restTemplate,
                url, params, getHttpHeaders(),
                new ParameterizedTypeReference<PulsarManagerResult>() {
                });
        int otherErrorCode = 2;
        LOGGER.debug("request pulsar manager result {}", pulsarManagerResult);
        if (!pulsarManagerResult.isSuccess() && pulsarManagerResult.getData() == otherErrorCode) {
            String errorMsg =
                    "create topic by pulsar manager error for groupId = %s, streamId = %s, sinkId = %s, errorMsg = %s";
            String errorInfo = String.format(errorMsg, sinkInfo.getInlongGroupId(), sinkInfo.getInlongStreamId(),
                    sinkInfo.getId(),
                    pulsarManagerResult.getErrorMessage());
            throw new BusinessException(errorInfo);
        }
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(REQUEST_HEADER_APP_ID, innerPulsarResourceConfig.getAppId());
        httpHeaders.add(REQUEST_HEADER_APP_SECRET, innerPulsarResourceConfig.getAppSecret());
        return httpHeaders;
    }

    private Map<String, Object> buildRequestBaseParams(PulsarDataNodeDTO pulsarDataNodeDTO,
            PulsarSinkDTO pulsarSinkDTO) {
        Map<String, Object> params = new HashMap<>();
        params.put("instanceId", pulsarDataNodeDTO.getInstanceId());
        params.put("cluster", pulsarDataNodeDTO.getClusterName());
        params.put("tenant", pulsarSinkDTO.getPulsarTenant());
        return params;
    }

    private PulsarDataNodeDTO getPulsarDataNodeInfo(SinkInfo sinkInfo) {

        String dataNodeName = sinkInfo.getDataNodeName();
        Preconditions.expectNotBlank(dataNodeName, ErrorCodeEnum.INVALID_PARAMETER,
                "Pulsar admin url not specified and data node is empty");
        PulsarDataNodeInfo dataNodeInfo = (PulsarDataNodeInfo) dataNodeOperateHelper.getDataNodeInfo(
                dataNodeName, sinkInfo.getSinkType());
        PulsarDataNodeDTO pulsarDataNodeDTO = new PulsarDataNodeDTO();
        CommonBeanUtils.copyProperties(dataNodeInfo, pulsarDataNodeDTO, true);
        return pulsarDataNodeDTO;
    }
}
