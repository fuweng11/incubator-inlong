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

package org.apache.inlong.agent.core.ha.lb;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.core.ha.JobHaDispatcher;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.ZkUtil;

import com.alibaba.fastjson.JSONObject;
import com.sun.management.OperatingSystemMXBean;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class LoadBalanceService implements AutoCloseable {

    public static final long MIBI = 1024 * 1024L;
    private final ScheduledExecutorService loadManagerExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("dbsync_load-manager"));
    public String localIp;
    public String clusterId;
    public String localLoadBalancePath;
    private Logger logger = LogManager.getLogger(JobHaDispatcherImpl.class);
    private JobHaDispatcher jobHaDispatcher;
    private volatile ScheduledExecutorService executor;
    private OperatingSystemMXBean systemBean;
    private int totalCpuLimit;
    private DbSyncHostUsage dbSyncHostUsage;
    private volatile LoadBalanceInfo localLoadBalanceInfo;

    public LoadBalanceService(JobHaDispatcher jobHaDispatcher, String localIp) {
        this.jobHaDispatcher = jobHaDispatcher;
        this.localIp = localIp;
        if (SystemUtils.IS_OS_LINUX) {
            dbSyncHostUsage = new LinuxDbSyncHostUsageImpl(1,
                    Optional.empty(),
                    loadManagerExecutor);
        } else {
            dbSyncHostUsage = new GenericDbSyncHostUsageImpl(1,
                    loadManagerExecutor);
        }

        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("loadBalance-reporter"));
        this.executor.scheduleAtFixedRate(this::reportLoadBalance, 0, 1, TimeUnit.MINUTES);

        this.systemBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.totalCpuLimit = getTotalCpuLimit();

    }

    private void reportLoadBalance() {
        try {
            SystemResourceUsage systemResourceUsage = dbSyncHostUsage.getDbSyncHostUsage();
            LoadBalanceInfo info = new LoadBalanceInfo();
            info.setIp(localIp);
            info.setMaxSyncIdsThreshold(jobHaDispatcher.getMaxSyncIdsThreshold());
            info.setBandwidthIn(systemResourceUsage.getBandwidthIn());
            info.setBandwidthOut(systemResourceUsage.getBandwidthOut());
            info.setCpu(systemResourceUsage.getCpu());
            info.setMemory(getHeapMemLoadInfo());
            info.setSystemMemory(systemResourceUsage.getSystemMemory());
            info.setSyncIdNum(jobHaDispatcher.getNeedToRunSyncIdSet().size());
            info.setTimeStamp(System.currentTimeMillis());
            ConfigDelegate configDelegate = jobHaDispatcher.getZkConfigDelegate();
            if (configDelegate != null && StringUtils.isNotEmpty(localLoadBalancePath)) {
                logger.info("report loadBalance info {}, path {}", JSONObject.toJSONString(info),
                        localLoadBalancePath);
                configDelegate.createEphemeralPathAndSetDataForClient(localLoadBalancePath,
                        JSONObject.toJSONString(info));
            }
            localLoadBalanceInfo = info;
        } catch (Exception e) {
            logger.info("reportLoadBalanceInfo error", e);
        }

    }

    private ResourceUsage getHeapMemLoadInfo() {
        long maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
        long memoryUsageInBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return new ResourceUsage((double) memoryUsageInBytes / MIBI,
                (double) maxHeapMemoryInBytes / MIBI);
    }

    public LoadBalanceInfo getLocalLoadBalanceInfo() {
        if (localLoadBalanceInfo == null) {
            this.reportLoadBalance();;
        }
        return localLoadBalanceInfo;
    }

    /**
     * atuo close
     *
     * @throws Exception ex
     */
    public void close() throws Exception {
        if (!this.executor.isShutdown()) {
            this.executor.shutdown();
        }
    }

    public synchronized void setClusterId(String clusterId) {
        this.clusterId = clusterId;
        localLoadBalancePath = ZkUtil.getCandidatePath(clusterId, localIp);
    }
}
