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

package org.apache.inlong.manager.plugin.util;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.plugin.oceanus.dto.JobBaseInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

@Slf4j
public class OceanusUtils {

    public static final String PROJECT_ID = "oceanus.job.projectId";
    public static final String FILE_ID = "sort.job.fileId";
    public static final String SORT_DIST_JAR_ID = "oceanus.job.sortDistJarId";
    public static final String SORT_SOURCE_CONNECT_ID = "oceanus.job.sortSourceConnectId";
    public static final String SORT_SINK_CONNECT_ID = "oceanus.job.sortSinkConnectId";
    public static final String STATE_TYPE = "oceanus.job.stateType";
    public static final String CHECK_POINT_MODE = "oceanus.job.checkpointMode";
    public static final String CHECK_POINT_TIMEOUT = "oceanus.job.checkpointTimeout";
    public static final String ENABLE_CHECK_POINTING = "oceanus.job.enableCheckpointing";
    public static final String CHECK_POINT_INTERVAL = "oceanus.job.checkpointInterval";

    public static final String CLUSTER_ID = "oceanus.job.clusterId";
    public static final String CPU_CORES = "oceanus.job.cpuCores";
    public static final String CPU_CORES_PER_TASK_MANAGER = "oceanus.job.cpuCoresPerTaskManager";
    public static final String JOB_MANAGER_CPU_CORES = "oceanus.job.jobManagerCpuCores";
    public static final String JOB_MANAGER_MEMORY_BYTES = "oceanus.job.jobmanagerMemoryBytes";
    public static final String MAX_PARALLELISM = "oceanus.job.maxParallelism";
    public static final String MEMORY_BYTES = "oceanus.job.memoryBytes";
    public static final String MEMORY_BYTES_PER_TASK_MANAGER = "oceanus.job.memoryBytesPerTaskmanager";
    public static final String PARALLELISM_PER_CORE = "oceanus.job.parallelismPerCore";

    public static JobBaseInfo initParamForOceanus(Map<String, String> kvConf) {
        JobBaseInfo jobBaseInfo = new JobBaseInfo();
        if (StringUtils.isNotBlank(kvConf.get(InlongConstants.SORT_JOB_ID))) {
            jobBaseInfo.setJobId(Long.valueOf(kvConf.get(InlongConstants.SORT_JOB_ID)));
        }
        if (StringUtils.isNotBlank(kvConf.get(FILE_ID))) {
            jobBaseInfo.setFileId(Long.valueOf(kvConf.get(FILE_ID)));
        }
        String projectId = kvConf.getOrDefault(PROJECT_ID, "14727");
        Preconditions.expectNotBlank(projectId, "oceanus projectId is is blank");
        jobBaseInfo.setProjectId(Long.valueOf(projectId));

        String sortDistJarId = kvConf.get(SORT_DIST_JAR_ID);
        jobBaseInfo.setSortDistJarId(sortDistJarId);
        String sortSourceConnectId = kvConf.get(SORT_SOURCE_CONNECT_ID);
        jobBaseInfo.setSortSourceConnectId(sortSourceConnectId);
        String sortSinkConnectId = kvConf.get(SORT_SINK_CONNECT_ID);
        jobBaseInfo.setSortSinkConnectId(sortSinkConnectId);
        String stateType = kvConf.get(STATE_TYPE);
        if (StringUtils.isNotBlank(stateType)) {
            jobBaseInfo.setStateType(stateType);
        }

        String checkPointMode = kvConf.get(CHECK_POINT_MODE);
        if (StringUtils.isNotBlank(checkPointMode)) {
            jobBaseInfo.setCheckpointMode(checkPointMode);
        }
        String checkPointTimeout = kvConf.get(CHECK_POINT_TIMEOUT);
        if (StringUtils.isNotBlank(checkPointTimeout)) {
            jobBaseInfo.setCheckpointTimeout(Integer.valueOf(checkPointTimeout));
        }
        String enableCheckPointing = kvConf.get(ENABLE_CHECK_POINTING);
        if (StringUtils.isNotBlank(enableCheckPointing)) {
            jobBaseInfo.setEnableCheckpointing(Boolean.valueOf(enableCheckPointing));
        }
        String checkPointInterval = kvConf.get(CHECK_POINT_INTERVAL);
        if (StringUtils.isNotBlank(checkPointInterval)) {
            jobBaseInfo.setCheckpointInterval(Integer.valueOf(checkPointInterval));
        }
        return jobBaseInfo;
    }

    public static JobBaseInfo initParamsForStandardMode(JobBaseInfo jobBaseInfo, Map<String, String> kvConf) {

        String clusterId = kvConf.getOrDefault(CLUSTER_ID, "20010");
        Preconditions.expectNotBlank(clusterId, "oceanus clusterId is is blank");
        jobBaseInfo.setClusterId(Long.valueOf(clusterId));

        jobBaseInfo.setCpuCores(Integer.valueOf(kvConf.getOrDefault(CPU_CORES, "2")));
        jobBaseInfo.setCpuCoresPerTaskmanager(Integer.valueOf(kvConf.getOrDefault(CPU_CORES_PER_TASK_MANAGER, "1")));
        jobBaseInfo.setResourceDescription("standard mode ressource");
        jobBaseInfo.setJobmanagerCpuCores(Integer.valueOf(kvConf.getOrDefault(JOB_MANAGER_CPU_CORES, "1")));
        jobBaseInfo.setJobmanagerMemoryBytes(Long.valueOf(kvConf.getOrDefault(JOB_MANAGER_MEMORY_BYTES, "2147483648")));
        jobBaseInfo.setMaxParallelism(Long.valueOf(kvConf.getOrDefault(MAX_PARALLELISM, "2048")));
        jobBaseInfo.setMemoryBytes(Long.valueOf(kvConf.getOrDefault(MEMORY_BYTES, "3221225472")));
        jobBaseInfo.setMemoryBytesPerTaskmanager(
                Long.valueOf(kvConf.getOrDefault(MEMORY_BYTES_PER_TASK_MANAGER, "1073741824")));
        jobBaseInfo.setParallelismPerCore(Integer.valueOf(kvConf.getOrDefault(PARALLELISM_PER_CORE, "1")));
        return jobBaseInfo;
    }
}
