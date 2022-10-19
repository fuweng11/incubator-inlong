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

package org.apache.inlong.agent.core.ha;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.core.ha.lb.LoadBalanceInfo;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.ZkUtil;
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

    private static final String EMPTY_IP_KEY = "empty_ip_key";
    private Logger logger = LogManager.getLogger(JobCoordinator.class);
    private Integer clusterId;

    private String clusterIdStr;

    private JobHaDispatcher jobHaDispatcher;

    private volatile boolean coordinatorRunning = false;

    private long coordinatorInterval = 1000L;

    private ScheduledExecutorService jobCoordinatorExecutor;

    private CopyOnWriteArraySet<String> syncIdSet = new CopyOnWriteArraySet<>();

    private volatile long syncIdListVersion = -1;

    private volatile boolean isNeedUpdateRunNodeMap = false;

    private Long lastPrintStatTimeStamp = 0L;

    private Gson gson = new Gson();

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
            logger.info("JobCoordinator has bean running!");
        }
    }

    public void stop() {
        coordinatorRunning = false;
        if (jobCoordinatorExecutor != null) {
            jobCoordinatorExecutor.shutdown();
            jobCoordinatorExecutor = null;
        }
    }

    public synchronized void updateServerIDList(Integer clusterId, long syncIdListVersion,
            CopyOnWriteArraySet<String> syncIdSet) {
        if (this.clusterId != clusterId || this.syncIdListVersion != syncIdListVersion) {
            this.clusterIdStr = String.valueOf(clusterId);
            this.clusterId = clusterId;
            this.syncIdSet.clear();
            this.syncIdSet.addAll(syncIdSet);
            this.syncIdListVersion = syncIdListVersion;
        }
    }

    public void setNeedUpdateRunNodeMap(boolean value) {
        isNeedUpdateRunNodeMap = value;
    }

    class CoordinatorTask implements Runnable {

        private long syncIdListVersion = -1;

        private long clusterId = -1;

        private List<String> syncIdList = new ArrayList<>();

        private boolean syncIdListIsChanged = false;

        /*
         * key: candidate ip
         * value : run serverId List
         */
        private ConcurrentHashMap<String, Set<String>> jobRunNodeMap = new ConcurrentHashMap<>();

        @Override
        public void run() {
            try {
                if (!jobHaDispatcher.isDbsyncUpdating() && jobHaDispatcher.isCoordinator()) {
                    synchronized (JobCoordinator.this) {
                        if ((syncIdListVersion != JobCoordinator.this.syncIdListVersion)
                                || (clusterId != JobCoordinator.this.clusterId)) {
                            syncIdList.clear();
                            syncIdList.addAll(JobCoordinator.this.syncIdSet);
                            syncIdListVersion = JobCoordinator.this.syncIdListVersion;
                            clusterId = JobCoordinator.this.clusterId;
                            syncIdListIsChanged = true;
                        }
                    }
                    if (syncIdList.size() == 0) {
                        logger.warn("CoordinatorTask running and serverIdList size is 0!");
                    }
                    /*
                     * query all infos for coordinate
                     */
                    /*
                     * 1. query all server id's run node info
                     * key: ip, value: server id set
                     */
                    if (jobRunNodeMap.size() == 0 || syncIdListIsChanged || isNeedUpdateRunNodeMap) {
                        if (isNeedUpdateRunNodeMap) {
                            isNeedUpdateRunNodeMap = false;
                        }
                        this.jobRunNodeMap.clear();
                        Map<String, Set<String>> jobRunNodeMap = getAllJobRunNodeInfo(syncIdList);
                        this.jobRunNodeMap.putAll(jobRunNodeMap);
                        syncIdListIsChanged = false;
                    }

                    /*
                     * 2. query all candidate load info
                     * key : ip, value : load
                     */
                    Map<String, LoadBalanceInfo> candidateLoadMap = getAllCandidateNodeLoad();

                    /*
                     * Assignment current server Id list num to load info
                     * and sort cadLoadSet
                     */
                    TreeSet<LoadBalanceInfo> cadLoadSet = null;
                    if (candidateLoadMap != null) {
                        cadLoadSet = new TreeSet<>();
                        Set<Map.Entry<String, LoadBalanceInfo>> candidateLoadSet = candidateLoadMap.entrySet();
                        for (Map.Entry<String, LoadBalanceInfo> entry : candidateLoadSet) {
                            Set<String> serverIdSet = jobRunNodeMap.get(entry.getKey());
                            if (serverIdSet != null) {
                                entry.getValue().setSyncIdNum(serverIdSet.size());
                            }
                            cadLoadSet.add(entry.getValue());
                        }
                    }

                    /*
                     * handle unassigned sync ids
                     */
                    Set<String> unassignedServerIdsSet = jobRunNodeMap.get(EMPTY_IP_KEY);
                    assignSyncIdsToDbSyncNode(unassignedServerIdsSet, cadLoadSet);

                    /*
                     * handle some candidates are offline case
                     */
                    Set<Map.Entry<String, Set<String>>> runNodeInfoEntrySet
                            = jobRunNodeMap.entrySet();
                    if (candidateLoadMap != null && candidateLoadMap.size() > 0) {
                        for (Map.Entry<String, Set<String>> entry : runNodeInfoEntrySet) {
                            if (candidateLoadMap.get(entry.getKey()) == null) {
                                logger.info("Candidate [{}] is offline, so need change run "
                                                + "syncIds [{}]", entry.getKey(),
                                        gson.toJson(entry.getValue()));
                                if (!assignSyncIdsToDbSyncNode(entry.getKey(), entry.getValue(),
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
                    printAllNodeRunJobInfo(syncIdList, jobRunNodeMap);
                } else {
                    logger.info("JobCoordinator current is updating !!!!");
                }
            } catch (Exception e) {
                logger.error("JobCoordinator has exception e = {}", e);
            }
        }

        private boolean assignSyncIdsToDbSyncNode(String fromIp,
                Set<String> unassignedSyncIdsSet,
                TreeSet<LoadBalanceInfo> cadLoadSet) throws Exception {
            if (unassignedSyncIdsSet == null || unassignedSyncIdsSet.size() == 0) {
                logger.info("JobCoordinator assignServerIdsToDbSyncNode unassignedServerIdsSet is "
                        + "null or size is 0 !");
                return false;
            }
            if (cadLoadSet == null || cadLoadSet.size() == 0) {
                logger.info("JobCoordinator assignServerIdsToDbSyncNode candidateSet is null or size "
                        + "is 0 !");
                return false;
            }
            ConfigDelegate configDelegate = getConfigDelegate();
            List<String> removeSyncIds = null;
            for (String syncId : unassignedSyncIdsSet) {
                if (configDelegate.checkPathIsExist(ConfigDelegate.ZK_GROUP,
                        ZkUtil.getCandidatePath(clusterIdStr, fromIp))) {
                    return false;
                }
                if (assignSyncIdToDbSyncNode(syncId, cadLoadSet)) {
                    if (removeSyncIds == null) {
                        removeSyncIds = new ArrayList<>();
                    }
                    removeSyncIds.add(syncId);
                }
            }
            if (removeSyncIds != null) {
                unassignedSyncIdsSet.removeAll(removeSyncIds);
            }
            return true;
        }

        private void assignSyncIdsToDbSyncNode(Set<String> unassignedSyncIdsSet,
                TreeSet<LoadBalanceInfo> cadLoadSet) {
            if (unassignedSyncIdsSet == null || unassignedSyncIdsSet.size() == 0) {
                logger.info("JobCoordinator assignSyncIdsToDbSyncNode unassignedServerIdsSet is "
                        + "null or size is 0 !");
                return;
            }
            if (cadLoadSet == null || cadLoadSet.size() == 0) {
                logger.info("JobCoordinator assignSyncIdsToDbSyncNode candidateSet is null or size "
                        + "is 0 !");
                return;
            }
            List<String> removeSyncIds = null;
            for (String syncId : unassignedSyncIdsSet) {
                if (assignSyncIdToDbSyncNode(syncId, cadLoadSet)) {
                    if (removeSyncIds == null) {
                        removeSyncIds = new ArrayList<>();
                    }
                    removeSyncIds.add(syncId);
                }
            }
            if (removeSyncIds != null) {
                unassignedSyncIdsSet.removeAll(removeSyncIds);
            }
        }

        private boolean assignSyncIdToDbSyncNode(String syncId,
                TreeSet<LoadBalanceInfo> cadLoadSet) {
            if (StringUtils.isEmpty(syncId) || cadLoadSet == null || cadLoadSet.size() == 0) {
                logger.info("JobCoordinator assign syncId [{}] or cadLoadSet is"
                        + " null or size is 0!", syncId);
                return false;
            }
            LoadBalanceInfo loadBalanceInfo = cadLoadSet.pollFirst();
            if (loadBalanceInfo != null && loadBalanceInfo.getSyncIdNum()
                    < loadBalanceInfo.getMaxSyncIdsThreshold()) {
                try {
                    String dbsyncNodeIp = loadBalanceInfo.getIp();
                    doChangeRunNodeInfo(dbsyncNodeIp, syncId);
                    loadBalanceInfo.setSyncIdNum(loadBalanceInfo.getSyncIdNum() + 1);
                    updateSyncIdsMapInfo(dbsyncNodeIp, syncId);
                    logger.info("JobCoordinator assign syncId [{}] to DbSync node [{}]!",
                            syncId, dbsyncNodeIp);
                    return true;
                } catch (Exception e) {
                    logger.error("assignServerIdToDbSyncNode has exception e = {}", e);
                } finally {
                    cadLoadSet.add(loadBalanceInfo);
                }
            } else {
                logger.error("JobCoordinator assign syncId [{}] to dbsync node [{}],"
                                + "job num is exceed max size [{}]! must add dbsync node!!!!",
                        syncId, loadBalanceInfo == null ? null : loadBalanceInfo.getIp(),
                        loadBalanceInfo.getMaxSyncIdsThreshold());
            }
            return false;
        }

        private void updateSyncIdsMapInfo(String toDbsyncNodeIp, String syncId) {
            Set<String> syncIdsSet = jobRunNodeMap.get(toDbsyncNodeIp);
            if (syncIdsSet == null) {
                syncIdsSet = new HashSet<String>();
                jobRunNodeMap.put(toDbsyncNodeIp, syncIdsSet);
            }
            syncIdsSet.add(syncId);
        }

        /**
         * change one syncId per time
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
                                String fromIp = loadBalanceInfo.getIp();
                                Set<String> syncIdSet = jobRunNodeMap.get(fromIp);
                                if (syncIdSet != null && syncIdSet.size() > 0) {
                                    String toChangeSyncId = syncIdSet.iterator().next();
                                    try {
                                        String syncId = syncIdSet.iterator().next();
                                        doChangeRunNodeInfo(firstNode.getIp(), syncId);
                                        syncIdSet.remove(syncId);
                                        updateSyncIdsMapInfo(firstNode.getIp(), syncId);
                                        logger.info("tryToChangeRunNodeByLoad success, from [{}]/[{}],"
                                                        + "to  [{}]/[{}]", fromIp, toChangeSyncId,
                                                firstNode.getIp(), toChangeSyncId);
                                    } catch (Exception e) {
                                        logger.error("tryToChangeRunNodeByLoad has exception, from [{}]/[{}], "
                                                        + "to  [{}]/[{}] e = {}", fromIp, toChangeSyncId,
                                                firstNode.getIp(), toChangeSyncId, e);
                                    }
                                }
                            } else if (firstNode != null) {
                                logger.warn("There is not any node can bee used to change, firstNode is "
                                                + "not match condition!"
                                                + " first node [{}]/[{}]!, last node [{}]/[{}]",
                                        firstNode.getIp(), firstNode.getSyncIdNum(),
                                        loadBalanceInfo.getIp(), loadBalanceInfo.getSyncIdNum());
                                break;
                            } else {
                                logger.warn("There is not any node can bee used to change,"
                                                + " first is null!, last node [{}]/[{}]",
                                        loadBalanceInfo.getIp(), loadBalanceInfo.getSyncIdNum());
                                break;
                            }
                        } catch (Exception e) {
                            logger.error("tryToChangeRunNodeByLoad has exception = {}", e);
                        }
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("tryToChangeRunNodeByLoad has exception = {}", e);
                }
                loadBalanceInfo = balanceInfoTreeSet.pollLast();
            }
        }

        private void doChangeRunNodeInfo(String ip, String syncId) throws Exception {
            JobRunNodeInfo info = new JobRunNodeInfo();
            info.setIp(ip);
            info.setTimeStamp(System.currentTimeMillis());
            ConfigDelegate configDelegate = getConfigDelegate();
            String runNodePath = ZkUtil.getJobRunNodePath(clusterIdStr, syncId);
            configDelegate.createPathAndSetData(ConfigDelegate.ZK_GROUP, runNodePath, info.toString());
            logger.info("doChangeRunNode, path = {}, runNodeIp = {}, syncId {}", runNodePath, ip, syncId);
        }

        /**
         * print current job execute stat info
         *
         * @param syncIdList
         * @param map
         */
        private void printAllNodeRunJobInfo(List syncIdList, Map<String, Set<String>> map) {
            long currentTimeStamp = Instant.now().toEpochMilli();
            if ((currentTimeStamp - lastPrintStatTimeStamp > (coordinatorInterval * 5))
                    || lastPrintStatTimeStamp == 0) {
                if (syncIdList != null && syncIdList.size() > 0) {
                    logger.info("JobCoordinator stat serverIdList = [{}]", gson.toJson(syncIdList));
                } else {
                    logger.info("JobCoordinator stat serverIdList size = 0");
                }
                if (map != null && map.size() > 0) {
                    Set<Map.Entry<String, Set<String>>> set = map.entrySet();
                    logger.info("JobCoordinator stat candidate size = {}", set.size());
                    for (Map.Entry<String, Set<String>> entry : set) {
                        int size = entry.getValue() == null ? 0 : entry.getValue().size();
                        logger.info("JobCoordinator stat ip = [{}], size = [{}] run server id "
                                        + "List [{}]",
                                entry.getKey(), size,
                                gson.toJson(entry.getValue()));
                    }
                } else {
                    logger.info("JobCoordinator stat Job node distribution map = 0");
                }
                lastPrintStatTimeStamp = currentTimeStamp;
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
                            Gson gson = new Gson();
                            loadBalanceInfo = gson.fromJson(String.valueOf(data), LoadBalanceInfo.class);
                            loadBalanceInfo.setSyncIdNum(0);
                            candidateNodeLoadMap.put(candidate, loadBalanceInfo);
                        } catch (Exception e) {
                            logger.error("GetAllCandidateNodeLoad parseObject exception path = {}, e "
                                    + "= {}", path, e);
                        }
                    } else {
                        logger.warn("Path [{}] GetAllCandidateNodeLoad info is null", path);
                        candidateNodeLoadMap.put(candidate, new LoadBalanceInfo());
                    }
                }
            }
            return candidateNodeLoadMap;
        }

        private Map<String, Set<String>> getAllJobRunNodeInfo(List<String> syncIdList) throws Exception {
            int totalSize = syncIdList.size();
            Map<String, Set<String>> runNodeInfoMap = new HashMap<>();
            Map<String, String> serverIpMap = new HashMap<>();
            if (totalSize > 0) {
                for (int i = 0; i < totalSize; i++) {
                    String syncId = syncIdList.get(i);
                    JobRunNodeInfo runNodeInfo = getJobRunNodeInfo(syncId);
                    String ip;
                    if (runNodeInfo != null) {
                        ip = runNodeInfo.getIp();
                        String lastIp = serverIpMap.put(syncId, ip);
                        if (lastIp != null) {
                            logger.error("JobCoordinator [{}] execute in multi-node one node {} and "
                                    + "other anther node {}", syncId, ip, lastIp);
                        }
                    } else {
                        ip = EMPTY_IP_KEY;
                    }
                    Set<String> serverIdSet = runNodeInfoMap.get(ip);
                    if (serverIdSet == null) {
                        serverIdSet = new HashSet<String>();
                        runNodeInfoMap.put(ip, serverIdSet);
                    }
                    if (!serverIdSet.contains(syncId)) {
                        serverIdSet.add(syncId);
                    }
                }
            }
            return runNodeInfoMap;
        }

        private ConfigDelegate getConfigDelegate() throws Exception {
            ConfigDelegate configDelegate = jobHaDispatcher.getZkConfigDelegate();
            if (configDelegate == null) {
                throw new Exception("configDelegate is null, zkUrl = "
                        + jobHaDispatcher.getZkUrl());
            }
            return configDelegate;
        }

        private JobRunNodeInfo getJobRunNodeInfo(String syncId) throws Exception {
            String runNodePath = ZkUtil.getJobRunNodePath(clusterIdStr, syncId);
            ConfigDelegate configDelegate = getConfigDelegate();
            JobRunNodeInfo runNodeInfo = null;
            byte[] data = configDelegate.getData(ConfigDelegate.ZK_GROUP, runNodePath);
            if (data != null) {
                try {
                    Gson gson = new Gson();
                    runNodeInfo = gson.fromJson(String.valueOf(data), JobRunNodeInfo.class);
                } catch (Exception e) {
                    logger.error("GetJobRunNodeInfo parseObject exception SyncId = {}, e = {}",
                            syncId, e);
                }
            } else {
                logger.warn("SyncId [{}] getRunNodeInfo info is null", syncId);
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
                        && runningNodeLoad.getCpu().percentUsage() > haNodeNeedChangeMaxThreshold) || (
                        runningNodeLoad.getMemory() != null
                                && runningNodeLoad.getMemory().percentUsage() > haNodeNeedChangeMaxThreshold) || (
                        runningNodeLoad.getBandwidthOut() != null
                                && runningNodeLoad.getBandwidthOut().percentUsage() > haNodeNeedChangeMaxThreshold) || (
                        runningNodeLoad.getBandwidthIn() != null
                                && runningNodeLoad.getBandwidthIn().percentUsage() > haNodeNeedChangeMaxThreshold)
                        || runningNodeLoad.getSyncsCapacityPercentUsage() > haNodeNeedChangeMaxThreshold) {
                    return true;
                }
            }
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
                if ((candidateNodeLoad.getSyncIdNum() < candidateNodeLoad.getMaxSyncIdsThreshold()) && (
                        candidateNodeLoad.getCpu() == null
                                || candidateNodeLoad.getCpu().percentUsage() < haNodeChangeCandidateThreshold) && (
                        candidateNodeLoad.getMemory() == null
                                || candidateNodeLoad.getMemory().percentUsage() < haNodeChangeCandidateThreshold) && (
                        candidateNodeLoad.getBandwidthOut() == null
                                || candidateNodeLoad.getBandwidthOut().percentUsage() < haNodeChangeCandidateThreshold)
                        && (candidateNodeLoad.getBandwidthIn() == null
                        || candidateNodeLoad.getBandwidthIn().percentUsage() > haNodeChangeCandidateThreshold)
                        && candidateNodeLoad.getSyncsCapacityPercentUsage() < 1.0F) {
                    return true;
                }
            }
            return false;
        }
    }
}
