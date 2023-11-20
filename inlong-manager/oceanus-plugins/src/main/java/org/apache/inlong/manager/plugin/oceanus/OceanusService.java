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

package org.apache.inlong.manager.plugin.oceanus;

import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.plugin.config.PluginRestTemplateConfig;
import org.apache.inlong.manager.plugin.oceanus.dto.JobBaseInfo;
import org.apache.inlong.manager.plugin.oceanus.dto.JobDetailInfo;
import org.apache.inlong.manager.plugin.oceanus.dto.OceanusConfig;
import org.apache.inlong.manager.plugin.oceanus.dto.OceanusFile;
import org.apache.inlong.manager.plugin.util.OceanusConfiguration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class OceanusService {

    // thread safe
    private static final Gson GSON = new GsonBuilder().create();

    private static final String CREATE_JOB_BASE_API = "/api/v2/projects/%s/jobs";
    private static final String QUERY_JOB_INFO_API = "/api/v2/projects/%s/jobs/%s/manifest";
    private static final String QUERY_JOB_STATUS_API = "/api/v2/projects/%s/jobs/%s/execution";
    private static final String CONFIG_JOB_JAR_API =
            "/api/v2/projects/%s/jobs/%s/program?newVersion=false&forceUpdate=false";
    private static final String CONFIG_JOB_JAR_VERSION_API =
            "/api/v2/projects/%s/jobs/%s/manifest?apply=true&newVersion=%s";
    private static final String CONFIG_JOB_RESOURCE_API = "/api/v2/projects/%s/jobs/%s/resource";
    private static final String CONFIG_JOB_ALARM_API = "/api/v2/projects/%s/jobs/%s/alarmConfig";

    private static final String START_JOB_API = "/api/v2/projects/%s/jobs/%s/start";
    private static final String STOP_JOB_API = "/api/v2/projects/%s/jobs/%s/stop";
    private static final String DELETE_JOB_API = " /api/v2/projects/%s/jobs/%s";

    private static final String UPLOAD_FILE_API = "/api/v2/projects/%s/files";
    private static final String ADD_FILE_VERSION_API = "/api/v2/projects/%s/files/%s/versions";
    private static final String UPDATE_FILE_VERSION_API = "/api/v2/projects/%s/files/%s";

    private static final String QUERY_FILE_API = "/api/v2/projects/%s/files/%s";

    private static final String LIST_FILE_API = "/api/v2/projects/%s/files?pageSize=9999&pageNum=1";
    private final OceanusConfig oceanusConfig;
    String SECURE_AUTHENTICATION = "secure-authentication";

    public OceanusService() throws Exception {
        OceanusConfiguration oceanusConfiguration = new OceanusConfiguration();
        oceanusConfig = oceanusConfiguration.getOceanusConfig();
    }

    public static boolean writeConfigToFile(String configJobDirectory, String configFileName, String content) {
        File file = new File(configJobDirectory);
        if (!file.exists()) {
            file.mkdirs();
        }
        String filePath = configJobDirectory + File.separator + configFileName;
        try {
            File existFile = new File(filePath);
            if (existFile.exists()) {
                existFile.delete();
            }
            FileWriter fileWriter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(content);
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            log.error("saveConfigToLocal failed", e);
            return false;
        }
        return true;
    }

    public Long createOceanusJob(JobBaseInfo jobInfo) throws Exception {
        Long result = null;
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String createJobUrl =
                    oceanusConfig.getOpenApi() + String.format(CREATE_JOB_BASE_API, jobInfo.getProjectId());
            Map<String, Object> params = new HashMap<>();
            params.put("name", jobInfo.getName());
            params.put("type", "JAR");
            params.put("description", "inlong");
            params.put("flinkVersion", "1.15");
            params.put("executionMode", "STREAMING");
            JobBaseInfo rsp = HttpUtils.postRequest(restTemplate, createJobUrl, params,
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<JobBaseInfo>() {
                    });
            result = rsp.getId();
            log.info("create oceanus job rsp={}", rsp);
        } catch (Exception e) {
            log.error("submit job from info {} failed: ", jobInfo, e);
            throw new Exception("submit job failed: " + e.getMessage());
        }
        return result;
    }

    public String configJob(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String configJobUrl =
                    oceanusConfig.getOpenApi() + String.format(CONFIG_JOB_JAR_VERSION_API, jobInfo.getProjectId(),
                            jobInfo.getJobId(), true);
            Map<String, Object> params = new HashMap<>();

            // program
            Map<String, Object> program = new HashMap<>();
            program.put("type", "jar");
            program.put("mainClassName", "org.apache.inlong.sort.Entrance");
            StringBuilder str = new StringBuilder();
            str.append("--job.name ").append(jobInfo.getName());
            str.append(" --group.info.file ").append(jobInfo.getName());
            str.append(" --checkpoint.interval ").append("60000");
            program.put("arguments", str);

            Map<String, Object> jarFileVersion = new HashMap<>();
            OceanusFile sortDistJar;
            if (StringUtils.isNotBlank(jobInfo.getSortDistJarId())) {
                sortDistJar = queryFile(jobInfo, Long.valueOf(jobInfo.getSortDistJarId()));
            } else {
                sortDistJar = getFileByName(jobInfo, oceanusConfig.getDefaultSortDistJarName());
            }
            jarFileVersion.put("fileId", sortDistJar.getId());
            jarFileVersion.put("projectId", jobInfo.getProjectId());
            jarFileVersion.put("version", sortDistJar.getCurrentVersion());
            program.put("jarFileVersion", jarFileVersion);
            HashMap<String, Object> artifactFileVersions = new HashMap<>();

            HashMap<String, Object> artifact1 = new HashMap<>();
            artifact1.put("projectId", jobInfo.getProjectId());
            OceanusFile sortSourceConnect;
            if (StringUtils.isNotBlank(jobInfo.getSortSourceConnectId())) {
                sortSourceConnect = queryFile(jobInfo, Long.valueOf(jobInfo.getSortSourceConnectId()));
            } else {
                sortSourceConnect = getFileByName(jobInfo, oceanusConfig.getDefaultSourceConnectName());
            }
            artifact1.put("fileId", sortSourceConnect.getId());
            artifact1.put("version", sortSourceConnect.getCurrentVersion());

            HashMap<String, Object> artifact2 = new HashMap<>();
            artifact2.put("projectId", jobInfo.getProjectId());
            OceanusFile sortSinkConncet;
            if (StringUtils.isNotBlank(jobInfo.getSortSinkConnectId())) {
                sortSinkConncet = queryFile(jobInfo, Long.valueOf(jobInfo.getSortSinkConnectId()));
            } else {
                sortSinkConncet = getFileByName(jobInfo, oceanusConfig.getDefaultSinkConnectName());
            }
            artifact2.put("fileId", sortSinkConncet.getId());
            artifact2.put("version", sortSinkConncet.getCurrentVersion());

            HashMap<String, Object> artifact3 = new HashMap<>();
            artifact3.put("projectId", jobInfo.getProjectId());
            artifact3.put("fileId", jobInfo.getFileId());
            OceanusFile oceanusFile = queryFile(jobInfo, jobInfo.getFileId());
            artifact3.put("version", oceanusFile.getCurrentVersion());

            artifactFileVersions.put(sortSourceConnect.getName(), artifact1);
            artifactFileVersions.put(sortSinkConncet.getName(), artifact2);
            artifactFileVersions.put(jobInfo.getName(), artifact3);

            program.put("artifactFileVersions", artifactFileVersions);

            // configuration
            Map<String, Object> configuration = new HashMap<>();
            configuration.put("stateType", jobInfo.getStateType());
            configuration.put("checkpointMode", jobInfo.getCheckpointMode());
            configuration.put("checkpointTimeout", jobInfo.getCheckpointTimeout());
            configuration.put("enableCheckpointing", jobInfo.getEnableCheckpointing());
            configuration.put("checkpointInterval", jobInfo.getCheckpointInterval());
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("log4j.logger.org.apache.hadoop.io.compress.CodecPool", "ERROR");
            properties.put("metrics.audit.enabled", false);
            configuration.put("properties", properties);
            params.put("program", program);
            params.put("configuration", configuration);
            log.info("config job params = {}", params);
            String rsp = HttpUtils.putRequest(restTemplate, configJobUrl, params,
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("config job rsp={}", rsp);
        } catch (Exception e) {
            log.error("config job info {} failed: ", jobInfo, e);
            throw new Exception("config job info failed: " + e.getMessage());
        }
        return null;
    }

    public void starJob(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String configResourceUrl = oceanusConfig.getOpenApi()
                    + String.format(START_JOB_API, jobInfo.getProjectId(), jobInfo.getJobId());

            String rsp = HttpUtils.putRequest(restTemplate, configResourceUrl, new HashMap<>(),
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("start job rsp={}", rsp);
        } catch (Exception e) {
            log.error("start job {} failed: ", jobInfo, e);
            throw new Exception("start job failed: " + e.getMessage());
        }
    }

    public String stopJob(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String stopJobUrl = oceanusConfig.getOpenApi()
                    + String.format(STOP_JOB_API, jobInfo.getProjectId(), jobInfo.getJobId());

            String rsp = HttpUtils.putRequest(restTemplate, stopJobUrl, new HashMap<>(),
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("stop job rsp={}", rsp);
            return rsp;
        } catch (Exception e) {
            log.error("stop job {} failed: ", jobInfo, e);
            throw new Exception("stop job failed: " + e.getMessage());
        }
    }

    public void deleteJob(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String deleteJobUrl =
                    oceanusConfig.getOpenApi()
                            + String.format(DELETE_JOB_API, jobInfo.getProjectId(), jobInfo.getJobId());

            String rsp = HttpUtils.request(restTemplate, deleteJobUrl, HttpMethod.DELETE, new HashMap<>(),
                    getHeader(jobInfo.getOperator()),
                    new ParameterizedTypeReference<String>() {
                    });
            log.info("delete job rsp={}", rsp);
        } catch (Exception e) {
            log.error("delete job {} failed: ", jobInfo, e);
            throw new Exception("stop job failed: " + e.getMessage());
        }
    }

    public String queryOceanusJob(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String createJobUrl =
                    oceanusConfig.getOpenApi()
                            + String.format(QUERY_JOB_INFO_API, jobInfo.getProjectId(), jobInfo.getJobId());
            Map<String, Object> params = new HashMap<>();
            params.put("name", jobInfo.getName());
            params.put("type", "JAR");
            params.put("description", "inlong");
            params.put("flinkVersion", "1.9");
            params.put("executionMode", "STREAMING");
            String rsp = HttpUtils.getRequest(restTemplate, createJobUrl, new HashMap<>(),
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("query oceanus job rsp={}", rsp);
        } catch (Exception e) {
            log.error("query job info {} failed: ", jobInfo, e);
            throw new Exception("query job info failed: " + e.getMessage());
        }
        return null;
    }

    public String queryJobStatus(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        JobDetailInfo rsp = new JobDetailInfo();
        try {
            String createJobUrl = oceanusConfig.getOpenApi()
                    + String.format(QUERY_JOB_STATUS_API, jobInfo.getProjectId(), jobInfo.getJobId());
            rsp = HttpUtils.getRequest(restTemplate, createJobUrl, new HashMap<>(),
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<JobDetailInfo>() {
                    });
            log.info("query oceanus job status rsp={}", rsp);
        } catch (Exception e) {
            log.error("query job status {} failed: ", jobInfo, e);
            throw new Exception("query job status failed: " + e.getMessage());
        }
        return rsp.getState();
    }

    public Long uploadFile(JobBaseInfo jobInfo, String dataFlow) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            path = path.substring(0, path.lastIndexOf(File.separator));
            log.info("gen path from {}", path);
            writeConfigToFile(path, jobInfo.getName(), dataFlow);
            OceanusFile configFile = getFileByName(jobInfo, jobInfo.getName());
            if (Objects.equals(configFile.getName(), jobInfo.getName())) {
                jobInfo.setFileId(configFile.getId());
            }
            if (jobInfo.getFileId() != null) {
                OceanusFile oceanusFile = queryFile(jobInfo, jobInfo.getFileId());
                if (!Objects.equals(oceanusFile.getType(), "FILE_NOT_FOUND")) {
                    updateFlie(jobInfo, oceanusFile.getCurrentVersion());
                    updateFileVersion(jobInfo, jobInfo.getFileId(), oceanusFile.getCurrentVersion() + 1);
                    return jobInfo.getFileId();
                }
            }

            String uploadFileUrl = oceanusConfig.getOpenApi() + String.format(UPLOAD_FILE_API, jobInfo.getProjectId());
            HashMap<String, Object> params = new HashMap<>();

            File file = new File(path + File.separator + jobInfo.getName());

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new FileSystemResource(file));
            body.add("description", "inlong file");
            body.add("fileName", jobInfo.getName());

            HttpHeaders header = getHeader(jobInfo.getOperator());
            header.setContentType(MediaType.MULTIPART_FORM_DATA);
            header.add("Content-Type", "multipart/form-data");
            HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(body, header);
            URI uri = URI.create(uploadFileUrl);
            ResponseEntity<OceanusFile> response = restTemplate.exchange(uri, HttpMethod.POST, entity,
                    OceanusFile.class);
            OceanusFile rsp = response.getBody();
            log.info("upload file rsp=：{}", rsp);
            return rsp.getId();
        } catch (Exception e) {
            log.error("upload file {} failed: ", jobInfo, e);
            throw new Exception("upload file failed: " + e.getMessage());
        }
    }

    public Long updateFlie(JobBaseInfo jobInfo, Long version) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String updateFileUrl = oceanusConfig.getOpenApi()
                    + String.format(ADD_FILE_VERSION_API, jobInfo.getProjectId(), jobInfo.getFileId());
            HashMap<String, Object> params = new HashMap<>();
            String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            path = path.substring(0, path.lastIndexOf(File.separator));
            File file = new File(path + File.separator + jobInfo.getName());

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", new FileSystemResource(file));
            body.add("description", "inlong file");
            body.add("fileName", jobInfo.getName());
            body.add("version", version + 1);
            HttpHeaders header = getHeader(jobInfo.getOperator());
            header.setContentType(MediaType.MULTIPART_FORM_DATA);
            header.add("Content-Type", "multipart/form-data");
            HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(body, header);
            URI uri = URI.create(updateFileUrl);
            ResponseEntity<String> response = restTemplate.exchange(uri, HttpMethod.POST, entity,
                    String.class);
            String rsp = response.getBody();
            log.info("update file rsp=：{}", rsp);
            return 1L;
        } catch (Exception e) {
            log.error("update file {} failed: ", jobInfo, e);
            throw new Exception("update file failed: " + e.getMessage());
        }
    }

    public OceanusFile queryFile(JobBaseInfo jobInfo, Long fileId) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String queryFileUrl =
                    oceanusConfig.getOpenApi() + String.format(QUERY_FILE_API, jobInfo.getProjectId(), fileId);
            log.info("query file params ={}", queryFileUrl);
            OceanusFile rsp = HttpUtils.getRequest(restTemplate, queryFileUrl, new HashMap<>(),
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<OceanusFile>() {
                    });
            log.info("query file rsp=：{}", rsp);
            return rsp;
        } catch (Exception e) {
            log.error("query file {} failed: ", jobInfo, e);
            throw new Exception("query file failed: " + e.getMessage());
        }
    }

    public JsonArray listFile(JobBaseInfo jobInfo, String fileName) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        JsonArray fileList = new JsonArray();
        try {
            String listFileUrl = oceanusConfig.getOpenApi() + String.format(LIST_FILE_API, jobInfo.getProjectId());
            log.info("list file path=：{}", listFileUrl);
            HashMap<String, Object> params = new HashMap<>();
            if (StringUtils.isNotBlank(fileName)) {
                params.put("keyword", fileName);
            }
            String rsp = HttpUtils.getRequest(restTemplate, listFileUrl, params,
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            JsonObject rspObj = GSON.fromJson(rsp, JsonObject.class);
            fileList = rspObj.getAsJsonArray("pageElements");
            return fileList;
        } catch (Exception e) {
            log.error("list file {} failed: ", jobInfo, e);
            throw new Exception("list file failed: " + e.getMessage());
        }
    }

    public OceanusFile getFileByName(JobBaseInfo jobInfo, String fileName) throws Exception {
        try {
            JsonArray fileList = listFile(jobInfo, fileName);
            OceanusFile file = new OceanusFile();
            for (JsonElement datum : fileList) {
                JsonObject record = datum.getAsJsonObject();
                file = GSON.fromJson(record.toString(), OceanusFile.class);
                if (Objects.equals(file.getName(), fileName)) {
                    break;
                }
            }
            log.info("get file rsp={}", file);
            return file;
        } catch (Exception e) {
            log.error("get file id by name {} failed: ", jobInfo, e);
            throw new Exception("get file id by namefailed: " + e.getMessage());
        }
    }

    public void updateFileVersion(JobBaseInfo jobInfo, Long fileId, Long version) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String queryFileUrl =
                    oceanusConfig.getOpenApi() + String.format(UPDATE_FILE_VERSION_API, jobInfo.getProjectId(), fileId);
            HashMap<String, String> params = new HashMap<>();
            params.put("version", String.valueOf(version));
            log.info("update file version params={}", params);
            String rsp = HttpUtils.putRequest(restTemplate, queryFileUrl, params,
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("update file version rsp=：{}", rsp);
        } catch (Exception e) {
            log.error("update file version {} failed: ", jobInfo, e);
            throw new Exception("update file version failed: " + e.getMessage());
        }
    }

    public String configResource(JobBaseInfo jobInfo) throws Exception {
        PluginRestTemplateConfig restTemplateConfig = new PluginRestTemplateConfig();
        RestTemplate restTemplate = restTemplateConfig.restTemplate();
        try {
            String configResourceUrl =
                    oceanusConfig.getOpenApi() + String.format(CONFIG_JOB_RESOURCE_API, jobInfo.getProjectId(),
                            jobInfo.getJobId());
            Map<String, Object> params = new HashMap<>();
            params.put("clusterId", jobInfo.getClusterId());
            params.put("cpuCores", jobInfo.getCpuCores());
            params.put("cpuCoresPerTaskmanager", jobInfo.getCpuCoresPerTaskmanager());
            params.put("description", jobInfo.getResourceDescription());
            params.put("jobmanagerCpuCores", jobInfo.getJobmanagerCpuCores());
            params.put("jobmanagerMemoryBytes", jobInfo.getJobmanagerMemoryBytes());
            params.put("maxParallelism", jobInfo.getMaxParallelism());
            params.put("memoryBytes", jobInfo.getMemoryBytes());
            params.put("memoryBytesPerTaskmanager", jobInfo.getMemoryBytesPerTaskmanager());
            params.put("parallelismPerCore", jobInfo.getParallelismPerCore());

            String rsp = HttpUtils.putRequest(restTemplate, configResourceUrl, params,
                    getHeader(jobInfo.getOperator()), new ParameterizedTypeReference<String>() {
                    });
            log.info("config resource rsp={}", rsp);
        } catch (Exception e) {
            log.error("config resource {} failed: ", jobInfo, e);
            throw new Exception("config resource failed: " + e.getMessage());
        }
        return null;
    }

    public HttpHeaders getHeader(String operator) throws Exception {
        HttpHeaders header = new HttpHeaders();
        try {
            TauthClient client = new TauthClient(oceanusConfig.getAuthUser(),
                    oceanusConfig.getAuthCmk());
            String encodedAuthentication = null;
            try {
                encodedAuthentication = client.getAuthentication("oceanus-api", operator);
            } catch (SecureException e) {
                log.error("generate tauth header error ", e);
            }
            header.add(SECURE_AUTHENTICATION, encodedAuthentication);
        } catch (Exception e) {
            log.error("get auth info failed", e);
            throw new Exception("get auth info failed" + e.getMessage());
        }
        return header;
    }

}
