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

package org.apache.inlong.agent.core.dbsync.ha;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.core.dbsync.ha.lb.LoadBalanceInfo;
import org.apache.inlong.agent.core.dbsync.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.dbsync.ha.zk.ZkUtil;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JobCoordinator {

    private Logger LOGGER = LogManager.getLogger(JobCoordinator.class);
    private Logger jobCoordinatorLogger = LogManager.getLogger("jobCoordinator");
    private static final String EMPTY_IP_KEY = "empty_ip_key";
    private Integer clusterId;
    private String clusterIdStr;
    private JobHaDispatcher jobHaDispatcher;
    private volatile boolean coordinatorRunning = false;
    private long coordinatorInterval = 1000L;
    private ScheduledExecutorService jobCoordinatorExecutor;
    private CopyOnWriteArraySet<String> dbJobIdSet = new CopyOnWriteArraySet<>();
    private volatile long dbJobIdListVersion = -1;
    private volatile boolean isNeedUpdateRunNodeMap = false;
    private Long lastPrintStatTimeStamp = 0L;
    private static final Gson GSON = new Gson();
    private float haNodeNeedChangeMaxThreshold = 0.7F;
    private float haNodeChangeCandidateThreshold = 0.5F;
    private float loadBalanceCheckLoadThreshold = 0.7F;
    private int loadBalanceCompareLoadUsageThreshold = 10;
    public JobCoordinator(Integer clusterId, long coordinatorInterval,
            JobHaDispatcher jobHaDispatcher, float haNodeNeedChangeMaxThreshold,
            float haNodeChangeCandidateThreshold, float loadBalanceCheckLoadThreshold,
            int loadBalanceCompareLoadUsageThreshold) {
        this.clusterId = clusterId;
        this.clusterIdStr = String.valueOf(clusterId);
        this.coordinatorInterval = coordinatorInterval;
        this.jobHaDispatcher = jobHaDispatcher;
        this.haNodeNeedChangeMaxThreshold = haNodeNeedChangeMaxThreshold;
        this.haNodeChangeCandidateThreshold = haNodeChangeCandidateThreshold;
        this.loadBalanceCheckLoadThreshold = loadBalanceCheckLoadThreshold;
        this.loadBalanceCompareLoadUsageThreshold = loadBalanceCompareLoadUsageThreshold;
    }

    public Integer getClusterId() {
        return clusterId;
    }

    public void start() {
        LoadBalanceInfo.CHECK_LOAD_THRESHOLD = this.loadBalanceCheckLoadThreshold;
        LoadBalanceInfo.COMPARE_LOAD_THRESHOLD = this.loadBalanceCompareLoadUsageThreshold;
        if (!coordinatorRunning) {
            coordinatorRunning = true;
            jobCoordinatorExecutor = Executors
                    .newSingleThreadScheduledExecutor(new DefaultThreadFactory("ha-state-check"
                            + "-monitor"));
            jobCoordinatorExecutor.scheduleWithFixedDelay(new CoordinatorTask(), 100,
                    coordinatorInterval,
                    TimeUnit.MILLISECONDS);
        } else {
            LOGGER.info("JobCoordinator has bean running!");
        }
    }

    public void stop() {
        coordinatorRunning = false;
        if (jobCoordinatorExecutor != null) {
            jobCoordinatorExecutor.shutdown();
            jobCoordinatorExecutor = null;
        }
    }

    public synchronized void updateServerIDList(Integer clusterId, long dbJobIdListVersion,
            CopyOnWriteArraySet<String> dbJobIdSet) {
        if ((clusterId != null && !clusterId.equals(this.clusterId)) || this.dbJobIdListVersion != dbJobIdListVersion) {
            this.clusterIdStr = String.valueOf(clusterId);
            this.clusterId = clusterId;
            this.dbJobIdSet.clear();
            this.dbJobIdSet.addAll(dbJobIdSet);
            this.dbJobIdListVersion = dbJobIdListVersion;
        }
    }

    public void setNeedUpdateRunNodeMap(boolean value) {
        isNeedUpdateRunNodeMap = value;
    }

    class CoordinatorTask implements Runnable {

        private long dbJobIdListVersion = -1;

        private Integer clusterId = -1;

        private List<String> dbJobIdList = new ArrayList<>();

        private boolean dbJobIdListIsChanged = false;

        /*
         * key: candidate ip value : run serverId List
         */
        private ConcurrentHashMap<String, Set<String>> jobRunNodeMap = new ConcurrentHashMap<>();

        @Override
        public void run() {
            try {
                if (!jobHaDispatcher.isDbsyncUpdating() && jobHaDispatcher.isCoordinator()) {
                    Long maxJobsSize = 0L;
                    synchronized (JobCoordinator.this) {
                        if ((dbJobIdListVersion != JobCoordinator.this.dbJobIdListVersion)
                                || (!clusterId.equals(JobCoordinator.this.clusterId))) {
                            dbJobIdList.clear();
                            dbJobIdList.addAll(JobCoordinator.this.dbJobIdSet);
                            dbJobIdListVersion = JobCoordinator.this.dbJobIdListVersion;
                            clusterId = JobCoordinator.this.clusterId;
                            dbJobIdListIsChanged = true;
                        }
                    }
                    if (dbJobIdList.size() == 0) {
                        LOGGER.warn("CoordinatorTask running and serverIdList size is 0!");
                    }
                    if (dbJobIdListIsChanged) {
                        List<String> unusedDbJobIds = handleUnusedDbJobIds(dbJobIdList);
                        printDbJobIdInfoWithVersion(dbJobIdList, unusedDbJobIds, dbJobIdListVersion);
                    }
                    /*
                     * query all infos for coordinate
                     */
                    /*
                     * 1. query all server name's run node info key: ip, value: server name set key : ip ,value : server
                     * Name set
                     */
                    if (jobRunNodeMap.size() == 0 || dbJobIdListIsChanged || isNeedUpdateRunNodeMap) {
                        if (isNeedUpdateRunNodeMap) {
                            isNeedUpdateRunNodeMap = false;
                        }
                        this.jobRunNodeMap.clear();
                        Map<String, Set<String>> jobRunNodeMap = getAllJobRunNodeInfo(dbJobIdList);
                        this.jobRunNodeMap.putAll(jobRunNodeMap);
                        dbJobIdListIsChanged = false;
                    }

                    /*
                     * 2. query all candidate load info key : ip, value : load
                     */
                    Map<String, LoadBalanceInfo> candidateLoadMap = getAllCandidateNodeLoad();

                    /*
                     * Assignment current server Id list num to load info and sort cadLoadSet
                     */
                    TreeSet<LoadBalanceInfo> cadLoadSet = null;
                    if (candidateLoadMap != null) {
                        cadLoadSet = new TreeSet<>();
                        Set<Map.Entry<String, LoadBalanceInfo>> candidateLoadSet = candidateLoadMap.entrySet();
                        for (Map.Entry<String, LoadBalanceInfo> entry : candidateLoadSet) {
                            Set<String> serverIdSet = jobRunNodeMap.get(entry.getKey());
                            if (serverIdSet != null) {
                                entry.getValue().setDbJobIdNum(serverIdSet.size());
                            }
                            maxJobsSize += entry.getValue().getMaxDbJobsThreshold();
                            cadLoadSet.add(entry.getValue());
                        }
                    }

                    /*
                     * handle unassigned dbJobIds
                     */
                    Set<String> unassignedServerIdsSet = jobRunNodeMap.get(EMPTY_IP_KEY);
                    assignDbJobIdToDbSyncNode(unassignedServerIdsSet, cadLoadSet);

                    /*
                     * handle some candidates are offline case
                     */
                    Set<Map.Entry<String, Set<String>>> runNodeInfoEntrySet = jobRunNodeMap.entrySet();
                    if (candidateLoadMap != null && candidateLoadMap.size() > 0) {
                        for (Map.Entry<String, Set<String>> entry : runNodeInfoEntrySet) {
                            if (candidateLoadMap.get(entry.getKey()) == null) {
                                LOGGER.info("Candidate [{}] is offline, so need change run "
                                        + "dbJobIds [{}]", entry.getKey(),
                                        GSON.toJson(entry.getValue()));
                                if (!assignDbJobIdToDbSyncNode(entry.getKey(), entry.getValue(),
                                        cadLoadSet)) {
                                    continue;
                                }
                            }
                        }
                    }
                    /*
                     * change run node by load info
                     */
                    tryToChangeRunNodeByLoad(cadLoadSet);
                    long currentTimeStamp = Instant.now().toEpochMilli();
                    if ((currentTimeStamp - lastPrintStatTimeStamp > (coordinatorInterval * 5))
                            || lastPrintStatTimeStamp == 0) {
                        printAllNodeRunJobInfo(dbJobIdList, jobRunNodeMap);
                        lastPrintStatTimeStamp = currentTimeStamp;
                        isNeedUpdateRunNodeMap = true;
                    }
                    LOGGER.info("JobCoordinator current clusterId = {}, dbJobIdListVersion = {}, "
                            + "maxJobsSize = {}, allJobSize = {}", clusterId, dbJobIdListVersion,
                            maxJobsSize, dbJobIdList.size());
                } else {
                    LOGGER.info("JobCoordinator current is updating !!!!");
                }
            } catch (Exception e) {
                LOGGER.error("JobCoordinator has exception e = {}", e);
            }
        }

        private List<String> handleUnusedDbJobIds(List<String> needRunDbJobIdList) {
            List<String> unUsedDbJobIds = new ArrayList<>();
            try {
                ConfigDelegate configDelegate = getConfigDelegate();
                String jobNodeParentPath = ZkUtil.getJobNodeParentPath(clusterIdStr);
                List<String> allJobList = configDelegate.getChildren(ConfigDelegate.ZK_GROUP,
                        jobNodeParentPath);
                if (allJobList != null) {
                    allJobList.stream().forEach((dbJobId) -> {
                        try {
                            if (needRunDbJobIdList != null && !needRunDbJobIdList.contains(dbJobId)) {
                                unUsedDbJobIds.add(dbJobId);
                                JobRunNodeInfo runNodeInfo = getJobRunNodeInfo(dbJobId);
                                if (runNodeInfo != null
                                        && (runNodeInfo.getRunningModel() != DBSyncJobConf.RUNNING_MODEL_STOPPED)) {
                                    LOGGER.info("handleUnusedDbJobIds change dbJobId {} runModel "
                                            + "from {} to 4.", dbJobId, runNodeInfo.getRunningModel());
                                    runNodeInfo.setRunningModel(DBSyncJobConf.RUNNING_MODEL_STOPPED);
                                    runNodeInfo.setHandleType(DBSyncJobConf.HANDLE_TYPE_AUTO);
                                    runNodeInfo.setTimeStamp(Instant.now().toEpochMilli());
                                    updateRunNodeInf(dbJobId, runNodeInfo);
                                }
                            }
                        } catch (Exception e1) {
                            LOGGER.error("handleUnusedDbJobIds has error !", e1);
                        }
                    });
                }

            } catch (Exception e) {
                LOGGER.error("handleUnusedDbJobIds has error !", e);
            }
            return unUsedDbJobIds;
        }

        private boolean assignDbJobIdToDbSyncNode(String fromIp,
                Set<String> unassignedDbJobIdSet,
                TreeSet<LoadBalanceInfo> cadLoadSet) throws Exception {
            if (unassignedDbJobIdSet == null || unassignedDbJobIdSet.size() == 0) {
                LOGGER.info("JobCoordinator assignDbJobIdToDbSyncNode unassignedDbJobIdSet is "
                        + "null or size is 0 !");
                return false;
            }
            if (cadLoadSet == null || cadLoadSet.size() == 0) {
                LOGGER.info("JobCoordinator assignDbJobIdToDbSyncNode candidateSet is null or size "
                        + "is 0 !");
                return false;
            }
            ConfigDelegate configDelegate = getConfigDelegate();
            List<String> removeDbJobIds = null;
            for (String dbJobId : unassignedDbJobIdSet) {
                if (configDelegate.checkPathIsExist(ConfigDelegate.ZK_GROUP,
                        ZkUtil.getCandidatePath(clusterIdStr, fromIp))) {
                    return false;
                }
                if (assignDbJobIdToDbSyncNode(dbJobId, cadLoadSet)) {
                    if (removeDbJobIds == null) {
                        removeDbJobIds = new ArrayList<>();
                    }
                    removeDbJobIds.add(dbJobId);
                }
            }
            if (removeDbJobIds != null) {
                unassignedDbJobIdSet.removeAll(removeDbJobIds);
            }
            return true;
        }

        private void assignDbJobIdToDbSyncNode(Set<String> unassignedDbJobIdSet,
                TreeSet<LoadBalanceInfo> cadLoadSet) {
            if (unassignedDbJobIdSet == null || unassignedDbJobIdSet.size() == 0) {
                LOGGER.info("JobCoordinator assignDbJobIdToDbSyncNode unassignedDbJobIdSet is "
                        + "null or size is 0 !");
                return;
            }
            if (cadLoadSet == null || cadLoadSet.size() == 0) {
                LOGGER.info("JobCoordinator assignDbJobIdToDbSyncNode candidateSet is null or size "
                        + "is 0 !");
                return;
            }
            List<String> removeDbJobIds = null;
            for (String dbJobId : unassignedDbJobIdSet) {
                if (assignDbJobIdToDbSyncNode(dbJobId, cadLoadSet)) {
                    if (removeDbJobIds == null) {
                        removeDbJobIds = new ArrayList<>();
                    }
                    removeDbJobIds.add(dbJobId);
                }
            }
            if (removeDbJobIds != null) {
                unassignedDbJobIdSet.removeAll(removeDbJobIds);
            }
        }

        private boolean assignDbJobIdToDbSyncNode(String dbJobId,
                TreeSet<LoadBalanceInfo> cadLoadSet) {
            if (StringUtils.isEmpty(dbJobId) || cadLoadSet == null || cadLoadSet.size() == 0) {
                LOGGER.info("JobCoordinator assign dbJobId [{}] or cadLoadSet is"
                        + " null or size is 0!", dbJobId);
                return false;
            }
            LoadBalanceInfo loadBalanceInfo = cadLoadSet.pollFirst();
            if (loadBalanceInfo != null && loadBalanceInfo.getDbJobIdNum() < loadBalanceInfo.getMaxDbJobsThreshold()) {
                try {
                    String registerKey = loadBalanceInfo.getIp();
                    if (StringUtils.isEmpty(registerKey)) {
                        return false;
                    }
                    doChangeRunNodeInfo(registerKey, dbJobId);
                    loadBalanceInfo.setDbJobIdNum(loadBalanceInfo.getDbJobIdNum() + 1);
                    updateDbJobIdMapInfo(registerKey, dbJobId);
                    LOGGER.info("JobCoordinator assign dbJobId [{}] to DbSync node [{}]!",
                            dbJobId, registerKey);
                    return true;
                } catch (Exception e) {
                    LOGGER.error("assignDbJobIdToDbSyncNode has exception e = {}", e);
                } finally {
                    cadLoadSet.add(loadBalanceInfo);
                }
            } else {
                LOGGER.error("JobCoordinator assign dbJobId [{}] to dbsync node [{}],"
                        + "job num is exceed max size [{}]! must add dbsync node!!!!",
                        dbJobId, loadBalanceInfo == null ? null : loadBalanceInfo.getIp(),
                        loadBalanceInfo.getMaxDbJobsThreshold());
            }
            return false;
        }

        private void updateDbJobIdMapInfo(String toDbsyncNodeIp, String dbJobId) {
            Set<String> dbJobIdSet = jobRunNodeMap.get(toDbsyncNodeIp);
            if (dbJobIdSet == null) {
                dbJobIdSet = new HashSet<String>();
                jobRunNodeMap.put(toDbsyncNodeIp, dbJobIdSet);
            }
            dbJobIdSet.add(dbJobId);
        }

        /**
         * change one dbJobId per time
         *
         * @param balanceInfoTreeSet balanceInfoTreeSet
         */
        private void tryToChangeRunNodeByLoad(TreeSet<LoadBalanceInfo> balanceInfoTreeSet) {
            if (balanceInfoTreeSet == null || balanceInfoTreeSet.size() == 0) {
                return;
            }
            LoadBalanceInfo loadBalanceInfo = balanceInfoTreeSet.pollLast();
            while (loadBalanceInfo != null) {
                try {
                    if (isNeedChangeRunNode(loadBalanceInfo)) {
                        LoadBalanceInfo firstNode = balanceInfoTreeSet.pollFirst();
                        try {
                            if (isNodeCanBeUsedToChange(firstNode)) {
                                String fromNode = loadBalanceInfo.getIp();
                                Set<String> dbJobIdSet = jobRunNodeMap.get(fromNode);
                                if (dbJobIdSet != null && dbJobIdSet.size() > 0) {
                                    String toChangeDbJobId = dbJobIdSet.iterator().next();
                                    try {
                                        String dbJobId = dbJobIdSet.iterator().next();
                                        doChangeRunNodeInfo(firstNode.getIp(), dbJobId);
                                        dbJobIdSet.remove(dbJobId);
                                        updateDbJobIdMapInfo(firstNode.getIp(), dbJobId);
                                        LOGGER.info("tryToChangeRunNodeByLoad success, from [{}]/[{}],"
                                                + "to  [{}]/[{}]", fromNode, toChangeDbJobId,
                                                firstNode.getIp(), toChangeDbJobId);
                                    } catch (Exception e) {
                                        LOGGER.error("tryToChangeRunNodeByLoad has exception, from [{}]/[{}], "
                                                + "to  [{}]/[{}] e = {}", fromNode, toChangeDbJobId,
                                                firstNode.getIp(), toChangeDbJobId, e);
                                    }
                                }
                            } else if (firstNode != null) {
                                LOGGER.warn("There is not any node can bee used to change, firstNode is "
                                        + "not match condition!"
                                        + " first node [{}]/[{}]!, last node [{}]/[{}]",
                                        firstNode.getIp(), firstNode.getDbJobIdNum(),
                                        loadBalanceInfo.getIp(), loadBalanceInfo.getDbJobIdNum());
                                break;
                            } else {
                                LOGGER.warn("There is not any node can bee used to change,"
                                        + " first is null!, last node [{}]/[{}]",
                                        loadBalanceInfo.getIp(), loadBalanceInfo.getDbJobIdNum());
                                break;
                            }
                        } catch (Exception e) {
                            LOGGER.error("tryToChangeRunNodeByLoad has exception = {}", e);
                        }
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.error("tryToChangeRunNodeByLoad has exception = {}", e);
                }
                loadBalanceInfo = balanceInfoTreeSet.pollLast();
            }
        }

        private void doChangeRunNodeInfo(String registerKey, String jobName) throws Exception {
            JobRunNodeInfo info = getJobRunNodeInfo(jobName);
            if (info == null) {
                info = new JobRunNodeInfo();
            }
            info.setIp(registerKey);
            info.setTimeStamp(System.currentTimeMillis());
            String runNodePath = updateRunNodeInf(jobName, info);
            LOGGER.info("doChangeRunNode, path = {}, runNodeIp = {}, jobName {}",
                    runNodePath, registerKey, jobName);
        }

        private void printDbJobIdInfoWithVersion(List<String> usedDbJobIds,
                List<String> unusedDbJobIds, long version) {
            if (usedDbJobIds != null) {
                jobCoordinatorLogger.info("DbJobIdListVersion = {} unusedDbJobIds size = {}, unusedDbJobIds = [{}]",
                        version, usedDbJobIds.size(), StringUtils.join(usedDbJobIds, ","));
            }
            if (unusedDbJobIds != null) {
                jobCoordinatorLogger.info("DbJobIdListVersion = {} unusedDbJobIds size = {}, unusedDbJobIds = [{}]",
                        version, unusedDbJobIds.size(), StringUtils.join(unusedDbJobIds, ","));
            }

        }

        private void printAllUnNormalModelRunJobInfo(Map<Integer, List<String>> unNormalRunJobMap) {
            if (unNormalRunJobMap != null) {
                unNormalRunJobMap.forEach((k, v) -> {
                    jobCoordinatorLogger.info("Run in model [{}], size = {} ,DbJobIds = [{}]",
                            k, v.size(), StringUtils.join(v, ","));
                });
            }
        }

        /**
         * print current job execute stat info
         *
         * @param dbJobIdList
         * @param map
         */
        private void printAllNodeRunJobInfo(List dbJobIdList, Map<String, Set<String>> map) {
            if (dbJobIdList != null && dbJobIdList.size() > 0) {
                jobCoordinatorLogger.info("JobCoordinator stat dbJobIdList = [{}]",
                        GSON.toJson(dbJobIdList));
            } else {
                jobCoordinatorLogger.info("JobCoordinator stat dbJobIdList size = 0");
            }
            if (map != null && map.size() > 0) {
                Set<Map.Entry<String, Set<String>>> set = map.entrySet();
                jobCoordinatorLogger.info("JobCoordinator stat candidate size = {}", set.size());
                for (Map.Entry<String, Set<String>> entry : set) {
                    int size = entry.getValue() == null ? 0 : entry.getValue().size();
                    jobCoordinatorLogger.info("JobCoordinator stat ip = [{}], size = [{}] run server name "
                            + "List [{}]",
                            entry.getKey(), size,
                            GSON.toJson(entry.getValue()));
                }
            } else {
                jobCoordinatorLogger.info("JobCoordinator stat Job node distribution map = 0");
            }
        }

        /**
         * key: candidate ip
         * value: dbsync node load info
         *
         * @return
         */
        private Map<String, LoadBalanceInfo> getAllCandidateNodeLoad() throws Exception {
            Map<String, LoadBalanceInfo> candidateNodeLoadMap = null;
            ConfigDelegate configDelegate = getConfigDelegate();
            String candidateNodePath = ZkUtil.getCandidateParentPath(clusterIdStr);
            List<String> candidateList = configDelegate.getChildren(ConfigDelegate.ZK_GROUP, candidateNodePath);
            if (candidateList != null && candidateList.size() > 0) {
                candidateNodeLoadMap = new HashMap<>();
                LoadBalanceInfo loadBalanceInfo = null;
                for (String candidate : candidateList) {
                    String path = ZkUtil.getZkPath(candidateNodePath, candidate);
                    byte[] data = configDelegate.getData(ConfigDelegate.ZK_GROUP, path);
                    if (data != null) {
                        try {
                            loadBalanceInfo = GSON.fromJson(new String(data), LoadBalanceInfo.class);
                            loadBalanceInfo.setDbJobIdNum(0);
                            candidateNodeLoadMap.put(candidate, loadBalanceInfo);
                        } catch (Exception e) {
                            LOGGER.error("GetAllCandidateNodeLoad parseObject exception path = {}, e "
                                    + "= {}", path, e);
                        }
                    } else {
                        LOGGER.warn("Path [{}] GetAllCandidateNodeLoad info is null", path);
                        candidateNodeLoadMap.put(candidate, new LoadBalanceInfo());
                    }
                }
            }
            return candidateNodeLoadMap;
        }

        private Map<String, Set<String>> getAllJobRunNodeInfo(List<String> dbJobIdList) throws Exception {
            int totalSize = dbJobIdList.size();
            Map<String, Set<String>> runNodeInfoMap = new HashMap<>();
            Map<String, String> serverIpMap = new HashMap<>();
            Map<Integer, List<String>> unNormalRunModelMap = new HashMap<>();
            if (totalSize > 0) {
                for (int i = 0; i < totalSize; i++) {
                    String dbJobId = dbJobIdList.get(i);
                    JobRunNodeInfo runNodeInfo = getJobRunNodeInfo(dbJobId);
                    String registerKey;
                    if (runNodeInfo != null && StringUtils.isNotEmpty(runNodeInfo.getIp())) {
                        handleUnNormalModelJob(runNodeInfo, dbJobId, unNormalRunModelMap);
                        registerKey = runNodeInfo.getIp();
                        String lastRegisterKey = serverIpMap.put(dbJobId, registerKey);
                        if (lastRegisterKey != null) {
                            LOGGER.error("JobCoordinator [{}] execute in multi-node one node {} and "
                                    + "other anther node {}", dbJobId, registerKey, lastRegisterKey);
                        }
                    } else {
                        registerKey = EMPTY_IP_KEY;
                    }
                    Set<String> serverIdSet = runNodeInfoMap.get(registerKey);
                    if (serverIdSet == null) {
                        serverIdSet = new HashSet<String>();
                        runNodeInfoMap.put(registerKey, serverIdSet);
                    }
                    if (!serverIdSet.contains(dbJobId)) {
                        serverIdSet.add(dbJobId);
                    }
                }
                printAllUnNormalModelRunJobInfo(unNormalRunModelMap);
            }
            return runNodeInfoMap;
        }

        private void handleUnNormalModelJob(JobRunNodeInfo runNodeInfo, String dbJobId,
                Map<Integer, List<String>> unNormalRunModelMap) {
            if (runNodeInfo == null) {
                return;
            }
            if (!DBSyncJobConf.RUNNING_MODEL_NORMAL.equals(runNodeInfo.getRunningModel())) {
                if (DBSyncJobConf.HANDLE_TYPE_AUTO.equals(runNodeInfo.getHandleType())) {
                    LOGGER.info("handleUnNormalModelJob change dbJobId {} runModel from 4 to 1.", dbJobId);
                    runNodeInfo.setRunningModel(DBSyncJobConf.RUNNING_MODEL_NORMAL);
                    runNodeInfo.setHandleType(DBSyncJobConf.HANDLE_TYPE_AUTO);
                    runNodeInfo.setTimeStamp(Instant.now().toEpochMilli());
                    updateRunNodeInf(dbJobId, runNodeInfo);
                } else {
                    List<String> unNormalDbJobIdList =
                            unNormalRunModelMap.get(runNodeInfo.getRunningModel());
                    if (unNormalDbJobIdList == null) {
                        unNormalDbJobIdList = new ArrayList<>();
                        unNormalRunModelMap.put(runNodeInfo.getRunningModel(),
                                unNormalDbJobIdList);
                    }
                    unNormalDbJobIdList.add(dbJobId);
                }
            }
        }

        private ConfigDelegate getConfigDelegate() throws Exception {
            ConfigDelegate configDelegate = jobHaDispatcher.getZkConfigDelegate();
            if (configDelegate == null) {
                throw new Exception("configDelegate is null, zkUrl = "
                        + jobHaDispatcher.getZkUrl());
            }
            return configDelegate;
        }

        private String updateRunNodeInf(String dbJobId, JobRunNodeInfo runNodeInfo) {
            if (runNodeInfo != null && StringUtils.isNotEmpty(dbJobId)) {
                try {
                    ConfigDelegate configDelegate = getConfigDelegate();
                    String runNodePath = ZkUtil.getJobRunNodePath(clusterIdStr, dbJobId);
                    configDelegate.createPathAndSetData(ConfigDelegate.ZK_GROUP, runNodePath,
                            GSON.toJson(runNodeInfo));
                    return runNodePath;
                } catch (Exception e) {
                    LOGGER.error("updateRunNodeInf [{}] has exception e", dbJobId, e);
                }
            }
            return null;
        }

        private JobRunNodeInfo getJobRunNodeInfo(String jobName) throws Exception {

            JobRunNodeInfo runNodeInfo = null;
            String runNodePath = jobName;
            try {
                runNodePath = ZkUtil.getJobRunNodePath(clusterIdStr, jobName);
                ConfigDelegate configDelegate = getConfigDelegate();

                byte[] data = configDelegate.getData(ConfigDelegate.ZK_GROUP, runNodePath);
                if (data != null) {
                    try {
                        runNodeInfo = GSON.fromJson(new String(data), JobRunNodeInfo.class);
                        if (runNodeInfo != null && runNodeInfo.getRunningModel() == null) {
                            runNodeInfo.setRunningModel(DBSyncJobConf.RUNNING_MODEL_NORMAL);
                        }
                    } catch (Exception e) {
                        LOGGER.error("GetJobRunNodeInfo parseObject exception dbJobId = {},"
                                + " path = {} e", jobName, runNodePath, e);
                    }
                } else {
                    LOGGER.warn("dbJobId [{}] path [{}] getRunNodeInfo info is null",
                            jobName, runNodePath);
                }
            } catch (Exception e) {
                LOGGER.warn("[{}] get runnode info has exception! ", runNodePath, e);
            }
            return runNodeInfo;
        }

        /**
         * check is or not change runningNode
         *
         * @param runningNodeLoad running Node load
         * @return true/false
         */
        private boolean isNeedChangeRunNode(LoadBalanceInfo runningNodeLoad) {
            if (runningNodeLoad != null) {
                if ((runningNodeLoad.getCpu() != null
                        && runningNodeLoad.getCpu().percentUsage() > haNodeNeedChangeMaxThreshold)
                        || (runningNodeLoad.getMemory() != null
                                && runningNodeLoad.getMemory().percentUsage() > haNodeNeedChangeMaxThreshold)
                        || (runningNodeLoad.getBandwidthOut() != null
                                && runningNodeLoad.getBandwidthOut().percentUsage() > haNodeNeedChangeMaxThreshold)
                        || (runningNodeLoad.getBandwidthIn() != null
                                && runningNodeLoad.getBandwidthIn().percentUsage() > haNodeNeedChangeMaxThreshold)
                        || runningNodeLoad.getSyncsCapacityPercentUsage() > haNodeNeedChangeMaxThreshold) {
                    LOGGER.info("isNeedChangeRunNode [{}]", true);
                    return true;
                }
            }
            LOGGER.info("isNeedChangeRunNode [{}]", false);
            return false;
        }

        /**
         * check is or not change runningNode
         *
         * @param candidateNodeLoad running Node load
         * @return true/false
         */
        private boolean isNodeCanBeUsedToChange(LoadBalanceInfo candidateNodeLoad) {
            if (candidateNodeLoad != null) {
                if ((candidateNodeLoad.getDbJobIdNum() < candidateNodeLoad.getMaxDbJobsThreshold())
                        && (candidateNodeLoad.getCpu() == null
                                || candidateNodeLoad.getCpu().percentUsage() < haNodeChangeCandidateThreshold)
                        && (candidateNodeLoad.getMemory() == null
                                || candidateNodeLoad.getMemory().percentUsage() < haNodeChangeCandidateThreshold)
                        && (candidateNodeLoad.getBandwidthOut() == null
                                || candidateNodeLoad.getBandwidthOut().percentUsage() < haNodeChangeCandidateThreshold)
                        && (candidateNodeLoad.getBandwidthIn() == null
                                || candidateNodeLoad.getBandwidthIn().percentUsage() < haNodeChangeCandidateThreshold)
                        && candidateNodeLoad.getSyncsCapacityPercentUsage() < 1.0F) {
                    LOGGER.info("isNodeCanBeUsedToChange [{}]", true);
                    return true;
                }
            }
            LOGGER.info("isNodeCanBeUsedToChange [{}]", false);
            return false;
        }
    }
}
