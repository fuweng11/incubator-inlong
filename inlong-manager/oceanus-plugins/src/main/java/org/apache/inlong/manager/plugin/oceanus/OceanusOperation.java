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

import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.plugin.flink.enums.OceanusJobStatus;
import org.apache.inlong.manager.plugin.oceanus.dto.JobBaseInfo;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Flink task operation, such restart or stop flink job.
 */
@Slf4j
public class OceanusOperation {

    private static final int MAX_RETRY_TIMES = 20;
    private final OceanusService oceanusService;

    public OceanusOperation(OceanusService oceanusService) {
        this.oceanusService = oceanusService;
    }

    /**
     * Start the Oceanus job, if the job id was not empty, restore it.
     */
    public void start(JobBaseInfo jobInfo, boolean isSync) throws Exception {
        Long jobId = jobInfo.getJobId();
        try {

            if (jobId == null) {
                jobId = oceanusService.createOceanusJob(jobInfo);
                jobInfo.setJobId(jobId);
            }
            log.info("Start job {} success in backend", jobId);
            oceanusService.configJob(jobInfo);
            if (!isSync) {
                oceanusService.configResource(jobInfo);
                oceanusService.starJob(jobInfo);
            }
        } catch (Exception e) {
            log.warn("submit flink job failed for {}", jobInfo, e);
            throw new Exception("submit flink job failed: " + e.getMessage());
        }
    }

    /**
     * Start the Oceanus job, if the job id was not empty, restore it.
     */
    public void stop(JobBaseInfo jobInfo) throws Exception {
        try {
            // stop a new task without savepoint
            oceanusService.stopJob(jobInfo);
            Preconditions.expectTrue(checkJobStatus(jobInfo, OceanusJobStatus.CANCELLED.getStatus()),
                    "Stop job failed");
        } catch (Exception e) {
            log.warn("stop oceanus job failed for {}", jobInfo, e);
            throw new Exception("stop oceanus job failed: " + e.getMessage());
        }
    }

    /**
     * Start the Oceanus job, if the job id was not empty, restore it.
     */
    public void delete(JobBaseInfo jobInfo) throws Exception {
        try {
            // Start a new task without savepoint
            oceanusService.deleteJob(jobInfo);
        } catch (Exception e) {
            log.warn("delete oceanus job failed for {}", jobInfo, e);
            throw new Exception("delete oceanus job failed: " + e.getMessage());
        }
    }

    public boolean checkJobStatus(JobBaseInfo jobInfo, String expectStatus) throws Exception {
        try {
            TimeUnit.SECONDS.sleep(15);
            String status = oceanusService.queryJobStatus(jobInfo);
            AtomicInteger retryTimes = new AtomicInteger(0);
            while (!Objects.equals(status, expectStatus) && retryTimes.get() < MAX_RETRY_TIMES) {
                log.info("check job status with now status={}, expect status={}, retryTimes={}, maxRetryTimes={}",
                        status, expectStatus, retryTimes.get(), MAX_RETRY_TIMES);
                // sleep 5 minute
                TimeUnit.SECONDS.sleep(3);
                retryTimes.incrementAndGet();
                status = oceanusService.queryJobStatus(jobInfo);
            }
            return Objects.equals(status, expectStatus);
        } catch (Exception e) {
            log.warn("check job status failed for {}", jobInfo, e);
            throw new Exception("check job status failed: " + e.getMessage());
        }
    }

}
