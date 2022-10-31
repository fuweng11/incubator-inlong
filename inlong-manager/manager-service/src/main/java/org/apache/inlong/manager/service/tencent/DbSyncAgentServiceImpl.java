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

package org.apache.inlong.manager.service.tencent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.agent.dbsync.DBServerInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncClusterInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncDumpPosition;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncDumpPosition.EntryPosition;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeatRequest;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncInitInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskFullInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.inlong.common.pojo.agent.dbsync.InitTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest.TaskInfoBean;
import org.apache.inlong.common.pojo.agent.dbsync.RunningTaskRequest;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.entity.tencent.DbSyncHeartbeatEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.tencent.DbSyncHeartbeatEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterInfo;
import org.apache.inlong.manager.pojo.node.mysql.MySQLDataNodeInfo;
import org.apache.inlong.manager.pojo.source.dbsync.AddFieldsRequest;
import org.apache.inlong.manager.pojo.source.dbsync.DbSyncTaskStatus;
import org.apache.inlong.manager.pojo.source.tencent.ha.HaBinlogSourceDTO;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.node.DataNodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * DbSync service interface implementation
 */
@Service
public class DbSyncAgentServiceImpl implements DbSyncAgentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbSyncAgentServiceImpl.class);
    // The value of the modulo 100 in the waiting status
    private static final int UNISSUED_STATUS = 2;
    // The value of the modulo 100 in the issued status
    private static final int ISSUED_STATUS = 3;
    private static final int MODULUS_100 = 100;
    private static final int TASK_FETCH_SIZE = 2;

    // --------------------------------------------------------------------------------------------
    // inner codes
    // --------------------------------------------------------------------------------------------
    // On reboot or first launch, pull all available tasks
    private static final String OP_INIT = "init";
    private static final String SUCCESS_SUFFIX = "_success";

    /**
     * The number of batch processing heartbeats, when this value is reached, get all tasks in intermediate status
     */
    private static final int HEARTBEAT_BATCH = 100;

    // The task was processed successfully
    private static final int TASK_SUCCESS = 0;
    // Task processing failed
    private static final int TASK_FAILED = 1;
    // Task is not scheduled
    private static final int TASK_NOT_SCHEDULE = -1;

    // Valid task cache, Key is the IP of the task
    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> ipTaskStatusMap;

    private final ExecutorService executorService = new ThreadPoolExecutor(
            5,
            10,
            10L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100),
            new ThreadFactoryBuilder().setNameFormat("async-agent-%s").build(),
            new CallerRunsPolicy());

    // Queue for heartbeat reporting requests - asynchronous processing
    private final LinkedBlockingQueue<DbSyncHeartbeatRequest> heartbeatQueue = new LinkedBlockingQueue<>();

    // field change queue (only supports adding fields)
    private final LinkedBlockingQueue<AddFieldsRequest> addFieldQueue = new LinkedBlockingQueue<>();

    // Cached task list
    private final Map<String, LinkedBlockingQueue<DbSyncTaskInfo>> ipTaskCacheMap = new ConcurrentHashMap<>(128);

    @Autowired
    private DataNodeService dataNodeService;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;
    @Autowired
    private DbSyncHeartbeatEntityMapper dbSyncHeartbeatMapper;

    /**
     * Start the heartbeat task
     */
    @PostConstruct
    private void startHeartbeatTask() {
        SaveHeartbeatTaskRunnable taskRunnable = new SaveHeartbeatTaskRunnable();
        ipTaskStatusMap = getDbSyncTaskStatusInfo();
        // AddFieldsTaskRunnable addFieldsTaskRunnable = new AddFieldsTaskRunnable();
        this.executorService.execute(taskRunnable);
        // this.executorService.execute(addFieldsTaskRunnable);
        LOGGER.info("dbsync heartbeat operate task started successfully");
    }

    @Override
    public Boolean heartbeat(DbSyncHeartbeatRequest request) {
        String ip = request.getIp();
        List<DbSyncHeartbeat> heartbeatList = request.getHeartbeats();
        if (heartbeatList == null || heartbeatList.size() == 0) {
            LOGGER.info("receive heartbeat from ip={}, but heartbeat msg is empty", ip);
            return true;
        }
        LOGGER.debug("receive heartbeat from ip={}, msg size={}", ip, heartbeatList.size());

        try {
            // process the heartbeat info async
            heartbeatQueue.offer(request);

            // all task IDs for all ephemeral status under the current IP
            ConcurrentHashMap<Integer, Integer> idStatusMap = ipTaskStatusMap != null ? ipTaskStatusMap.get(ip) : null;
            if (idStatusMap == null) {
                LOGGER.info("success report dbsync heartbeat for ip={}, task cache is null", request.getIp());
                return true;
            }

            // all tasks' IDs from the heartbeat request, corresponding to the IDs in the stream_source table
            Set<Integer> taskIdSet = new HashSet<>();
            // according to the reported task ID and status, modify the stream_source status
            for (DbSyncHeartbeat heartbeat : heartbeatList) {
                List<String> taskIds = heartbeat.getTaskIds();
                if (taskIds == null || taskIds.size() == 0) {
                    continue;
                }

                for (String idStr : taskIds) {
                    Integer id = Integer.parseInt(idStr);
                    taskIdSet.add(id);
                    Integer status = idStatusMap.get(id);
                    // Change the starting / unfrozen status to the normal status(101)
                    boolean isNewStatus = (status != null) && (status.equals(SourceStatus.AGENT_ISSUED_CREATE.getCode())
                            || status.equals(SourceStatus.AGENT_ISSUED_START.getCode()));
                    if (isNewStatus) {
                        sourceMapper.updateStatus(id, SourceStatus.AGENT_NORMAL.getCode(), null, null);
                    }
                }
            }

            // if the reported IDs does not contain the task to be deleted / to be frozen,
            // then modify the tasks in these temporary status to frozen / disabled
            Set<Entry<Integer, Integer>> localTaskStatusSet = idStatusMap.entrySet();
            for (Entry<Integer, Integer> entry : localTaskStatusSet) {
                Integer localTaskId = entry.getKey();
                Integer localTaskStatus = entry.getValue();
                if (!taskIdSet.contains(localTaskId)) {
                    // change the to be frozen to frozen(102)
                    if (Objects.equals(localTaskStatus, SourceStatus.AGENT_ISSUED_STOP.getCode())) {
                        sourceMapper.updateStatus(localTaskId, SourceStatus.AGENT_FREEZE.getCode(), null, null);
                    } else if (Objects.equals(localTaskStatus, SourceStatus.AGENT_ISSUED_DELETE.getCode())) {
                        // change the to be deleted to disabled(99)
                        sourceMapper.updateStatus(localTaskId, SourceStatus.AGENT_DISABLE.getCode(), null, null);
                    }
                }
            }

            LOGGER.debug("success report dbsync heartbeat for ip={}", request.getIp());
            return true;
        } catch (Throwable t) {
            LOGGER.error("update dbsync heartbeat error", t);
            return false;
        }
    }

    @Override
    public DbSyncHeartbeat getHeartbeat(Integer id, Integer dataNodeId) {
        // the data node must exist
        if (dataNodeService.get(dataNodeId) == null) {
            throw new BusinessException("no db node found with id=" + dataNodeId);
        }

        // query the latest heartbeat by the taskId and serverId
        DbSyncHeartbeat message = new DbSyncHeartbeat();
        DbSyncHeartbeatEntity entity = dbSyncHeartbeatMapper.getHeartbeat(id, dataNodeId);
        if (entity == null) {
            LOGGER.warn("no heartbeat message found with id={}, nodeId={}", id, dataNodeId);
            return message;
        }
        message.setInstance(entity.getInstance());
        message.setServerId(entity.getServerId());
        message.setCurrentDb(entity.getCurrentDb());
        message.setUrl(entity.getUrl());
        message.setBackupUrl(entity.getBackupUrl());
        message.setAgentStatus(entity.getAgentStatus());
        if (entity.getTaskIds() != null) {
            message.setTaskIds(Arrays.asList(entity.getTaskIds().split(InlongConstants.COMMA)));
        }
        message.setDumpIndex(entity.getDbDumpIndex());
        if (StringUtils.isNotBlank(entity.getDumpPosition())) {
            message.setDumpPosition(JsonUtils.parseObject(entity.getDumpPosition(), DbSyncDumpPosition.class));
        }
        if (StringUtils.isNotBlank(entity.getMaxLogPosition())) {
            message.setMaxLogPosition(JsonUtils.parseObject(entity.getMaxLogPosition(), DbSyncDumpPosition.class));
        }

        message.setErrorMsg(entity.getErrorMsg());
        message.setReportTime(entity.getReportTime());

        LOGGER.debug("success get heartbeat message by clusterId={}, serverId={}", id, dataNodeId);
        return message;
    }

    @Override
    public DbSyncInitInfo getInitInfo(InitTaskRequest request) {
        LOGGER.info("begin to get init info for: " + request);

        List<String> nodeNames = sourceMapper.selectNodeNames(request.getClusterName(), SourceType.HA_BINLOG);
        if (CollectionUtils.isEmpty(nodeNames)) {
            LOGGER.warn("return null - not any ha_binlog source found for: {}", request);
            return null;
        }
        DbSyncInitInfo initInfo = new DbSyncInitInfo();
        initInfo.setServerNames(nodeNames);

        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(
                request.getClusterTag(), null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters)) {
            throw new BusinessException("zk cluster for ha not found for cluster tag=" + request.getClusterTag());
        }
        initInfo.setZkUrl(zkClusters.get(0).getUrl());

        AgentClusterInfo clusterInfo = (AgentClusterInfo) clusterService.getOne(
                request.getClusterTag(), request.getClusterName(), ClusterType.AGENT);
        initInfo.setCluster(DbSyncClusterInfo.builder()
                .parentId(clusterInfo.getId())
                .clusterName(clusterInfo.getName())
                .serverVersion(clusterInfo.getServerVersion())
                .build());

        LOGGER.info("success to get init info for: {}, result: {}", request, initInfo);
        return initInfo;
    }

    @Override
    public DbSyncTaskFullInfo getRunningTask(RunningTaskRequest request) {
        LOGGER.info("begin to get running tasks for request: " + request);
        String serverName = request.getServerName();
        if (serverName == null) {
            LOGGER.warn("get task end, as the serverName is null");
            return null;
        }

        // check the dbsync cluster, zk cluster info
        DbSyncClusterInfo dbSyncCluster = request.getDbSyncCluster();
        if (dbSyncCluster == null || dbSyncCluster.getClusterName() == null) {
            throw new BusinessException("dbsync cluster or clusterId cannot be null when getRunningTask");
        }
        ClusterInfo clusterInfo = clusterService.get(dbSyncCluster.getParentId());
        if (!(clusterInfo instanceof AgentClusterInfo)) {
            throw new BusinessException("inlong cluster type is not AGENT for id=" + dbSyncCluster.getParentId());
        }
        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(
                request.getClusterTag(), null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters)) {
            throw new BusinessException("zk cluster for ha not found for cluster tag=" + request.getClusterTag());
        }

        DbSyncTaskFullInfo fullTaskInfo = new DbSyncTaskFullInfo();
        fullTaskInfo.setZkUrl(zkClusters.get(0).getUrl());

        // pull all tasks for this running server, including normal and updated tasks
        String clusterName = dbSyncCluster.getClusterName();
        List<Integer> taskIdList = sourceMapper.selectValidIds(SourceType.HA_BINLOG, clusterName,
                Collections.singletonList(serverName));
        List<DbSyncTaskInfo> taskList = this.getTaskByIdList(request.getIp(), taskIdList, OP_INIT);
        fullTaskInfo.setTaskInfoList(taskList);

        // if the cluster version changed, should issue all changed server ids
        Integer serverVersion = ((AgentClusterInfo) clusterInfo).getServerVersion();
        if (!Objects.equals(dbSyncCluster.getServerVersion(), serverVersion)) {
            dbSyncCluster.setServerVersion(serverVersion);
            fullTaskInfo.setChangedServers(sourceMapper.selectNodeNames(clusterName, SourceType.HA_BINLOG));
        }
        fullTaskInfo.setCluster(dbSyncCluster);

        LOGGER.info("get running tasks success, changed server names: {}, cluster: {}, task size: {}, request: {}",
                fullTaskInfo.getChangedServers(), dbSyncCluster, taskList.size(), request);
        return fullTaskInfo;
    }

    @Override
    public DbSyncTaskFullInfo reportAndGetTask(ReportTaskRequest request) {
        String ip = request.getIp();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("report and pull dbsync task info from ip={}, request={}", ip, request);
        }

        // 1. deal with the status heartbeat
        this.dealTaskStatus(request);
        // 2. pull the new tasks
        return this.getFullTaskInfo(request);
    }

    @Override
    public Boolean addFields(AddFieldsRequest request) {
        if (CollectionUtils.isEmpty(request.getFields())) {
            LOGGER.warn("dbsync add field request is empty, just return");
            return true;
        }
        LOGGER.info("received dbsync add fields request: {}", request);
        return addFieldQueue.add(request);
    }

    /**
     * Query all tasks in the initial+intermediate status (not issued, not started, and being issued)
     * according to the IP and serverNames,
     *
     * <p/> Filter 99/102/103 (disable/stopped/failed) tasks,
     * <p/> Do not filter is_deleted > 0, because it may be deleted, but the status is still
     * to_be_issued_deleted(204) / been_issued_deleted(304)
     *
     * If there is no node(IP) in the cluster for this request, or the node's parentId is different from
     * the reported parentId, it means that this IP has been offline from the original cluster, then all
     * task IDs reported this time need to be marked as offline task IDs
     */
    private DbSyncTaskFullInfo getFullTaskInfo(ReportTaskRequest request) {
        DbSyncTaskFullInfo fullTaskInfo = new DbSyncTaskFullInfo();

        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(
                request.getClusterTag(), null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters)) {
            throw new BusinessException("zk cluster for ha not found for cluster tag=" + request.getClusterTag());
        }

        fullTaskInfo.setZkUrl(zkClusters.get(0).getUrl());

        DbSyncClusterInfo oldCluster = request.getDbSyncCluster();
        String ip = request.getIp();
        InlongClusterNodeEntity clusterNode = clusterNodeMapper.selectByParentIdAndIp(oldCluster.getParentId(), ip);
        // if clusterNode is null, it means the IP goes offline from the original cluster
        String oldName = oldCluster.getClusterName();
        if (clusterNode == null) {
            List<String> oldServerNames = sourceMapper.selectNodeNames(oldName, SourceType.HA_BINLOG);
            fullTaskInfo.setOfflineServers(oldServerNames);
            LOGGER.info("ip {} has already removed, offline all server names {}", ip, oldServerNames);
            return fullTaskInfo;
        }

        // after the current IP goes offline, it is added to another cluster,
        // offline the original serverNames is offline, and issue the latest serverNames
        ClusterInfo curCluster = clusterService.get(oldCluster.getParentId());
        if (!(curCluster instanceof AgentClusterInfo)) {
            throw new BusinessException("inlong cluster type is not AGENT for id=" + oldCluster.getParentId());
        }

        String curName = curCluster.getName();
        if (!Objects.equals(curName, oldName)) {
            List<String> oldServerNames = sourceMapper.selectNodeNames(oldName, SourceType.HA_BINLOG);
            fullTaskInfo.setOfflineServers(oldServerNames);
            List<String> curServerNames = sourceMapper.selectNodeNames(curName, SourceType.HA_BINLOG);
            fullTaskInfo.setChangedServers(curServerNames);
            LOGGER.info("ip {} was added to cluster {}, offline all servers {}, and get newly servers {}",
                    ip, curName, oldServerNames, curServerNames);
            return fullTaskInfo;
        }

        // get the task IDs that still need to be run for the current IP
        List<String> runningServerNames = request.getServerNames();
        if (CollectionUtils.isNotEmpty(runningServerNames)) {
            List<Integer> taskIds = sourceMapper.selectValidIds(SourceType.HA_BINLOG, curName, runningServerNames);
            if (CollectionUtils.isEmpty(taskIds)) {
                LOGGER.warn("task ids is empty for ip={} serverNames={}", ip, runningServerNames);
            } else {
                List<DbSyncTaskInfo> taskInfoList = this.getTaskByIdList(ip, taskIds, null);
                fullTaskInfo.setTaskInfoList(taskInfoList);
                LOGGER.info("success to get tasks for ip={}, serverNames={}, result size={}",
                        ip, runningServerNames, taskInfoList.size());
            }
        }

        // serverVersion associated with the cluster was changed, then pulls all associated serverNames
        Integer oldVersion = oldCluster.getServerVersion();
        Integer curVersion = ((AgentClusterInfo) curCluster).getServerVersion();
        if (!Objects.equals(curVersion, oldVersion)) {
            List<String> curServerNames = sourceMapper.selectNodeNames(curName, SourceType.HA_BINLOG);
            fullTaskInfo.setChangedServers(curServerNames);
            LOGGER.info("server version was changed from {} to {} for ip {} in cluster {}, get all servers {}",
                    oldVersion, curVersion, ip, curName, curServerNames);
        }

        oldCluster.setServerVersion(curVersion);
        fullTaskInfo.setCluster(oldCluster);

        return fullTaskInfo;
    }

    /**
     * Query all sources according to the source IDs and return new tasks
     *
     * @param idList ID list
     * @param opType operation type, if it is init, all tasks will be pulled
     * @return task list
     * @apiNote Those result tasks has been issued, so change the source status in 20x to 30x
     */
    private List<DbSyncTaskInfo> getTaskByIdList(String ip, List<Integer> idList, String opType) {
        LOGGER.debug("begin to get task for ip={} with opType={}, idList={}", ip, opType, idList);
        if (CollectionUtils.isEmpty(idList)) {
            return Collections.emptyList();
        }
        // only querying the tasks that its belongs inlong stream status is [Configuration Successful]
        List<StreamSourceEntity> sourceEntities = sourceMapper.selectTaskByIdList(idList);
        if (CollectionUtils.isEmpty(sourceEntities)) {
            LOGGER.warn("task info is null for idList=" + idList);
            return Collections.emptyList();
        }

        List<Integer> skipTaskIds = new ArrayList<>();
        List<DbSyncTaskInfo> resultList = new ArrayList<>(sourceEntities.size());
        for (StreamSourceEntity sourceEntity : sourceEntities) {
            DbSyncTaskInfo taskInfo = fulfillTaskInfo(sourceEntity);
            int id = sourceEntity.getId();
            int status = taskInfo.getStatus();
            // 1. to be issued status(20x) - after modifying the task / DB server, status will change to intermediate
            // 2. been issued status(30x)
            if (status / 100 == UNISSUED_STATUS || status / 100 == ISSUED_STATUS) {
                taskInfo.setStatus(status % 100);
            } else if (SourceStatus.AGENT_NORMAL.getCode() == status) {
                // if not restart / first time start, ignore the normal tasks(status=101),
                // otherwise pull the normal tasks
                if (!OP_INIT.equals(opType)) {
                    skipTaskIds.add(id);
                    continue;
                }
            } else {
                // ignore tasks that with other status
                LOGGER.info("skip un-normal task, id={}", id);
                continue;
            }

            // change the status: from 20X to 30X
            if ((status / 100) == UNISSUED_STATUS) {
                int nextStatus = status % 100 + 300;
                sourceMapper.updateStatus(id, nextStatus, "dbsync task publish success", false);

                // if the user set the start position, it will be parsed and issued.
                // in the process of status from 20x to 30x, it will only be issued this time
                try {
                    String positionStr = sourceEntity.getStartPosition();
                    if (StringUtils.isNotBlank(positionStr) && !positionStr.equalsIgnoreCase("null")) {
                        DbSyncDumpPosition position = JsonUtils.parseObject(positionStr, DbSyncDumpPosition.class);
                        assert position != null;
                        EntryPosition entryPosition = position.getEntryPosition();
                        String journalName = entryPosition.getJournalName();
                        if (StringUtils.isNotBlank(journalName) && journalName.endsWith(SUCCESS_SUFFIX)) {
                            // "_success" suffix indicates that it has been issued and will not be issued again
                            taskInfo.setStartPosition("");
                        } else {
                            // after publishing, add a suffix and save it,
                            // to avoid it being re-issued after updating other fields
                            StreamSourceEntity entity = new StreamSourceEntity();
                            entity.setId(id);
                            entryPosition.setJournalName(journalName + SUCCESS_SUFFIX);
                            position.setEntryPosition(entryPosition);
                            entity.setStartPosition(JsonUtils.toJsonString(position));
                            sourceMapper.updateByPrimaryKeySelective(entity);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("start position is invalid for id={}, skip to parse and issue", id);
                }
            }

            resultList.add(taskInfo);
        }

        LOGGER.debug("ip={} with opType={}, skipped task id list={}", ip, opType, skipTaskIds);
        return resultList;
    }

    /**
     * Fulfill task info from the source entity.
     *
     * TODO need get cluster address? -  How do specify the Agent's Sink is DataProxy/TubeMQ/Pulsar?
     */
    private DbSyncTaskInfo fulfillTaskInfo(StreamSourceEntity sourceEntity) {
        String groupId = sourceEntity.getInlongGroupId();
        String streamId = sourceEntity.getInlongStreamId();
        DbSyncTaskInfo taskInfo = DbSyncTaskInfo.builder()
                .id(sourceEntity.getId())
                .inlongGroupId(groupId)
                .inlongStreamId(streamId)
                .serverName(sourceEntity.getDataNodeName())
                .status(sourceEntity.getStatus())
                .version(sourceEntity.getVersion())
                .build();

        HaBinlogSourceDTO sourceDTO = HaBinlogSourceDTO.getFromJson(sourceEntity.getExtParams());
        taskInfo.setDbName(sourceDTO.getDbName());
        taskInfo.setTableName(sourceDTO.getTableName());
        taskInfo.setCharset(sourceDTO.getCharset());
        taskInfo.setSkipDelete(sourceDTO.getSkipDelete());

        // get all cluster node IPs
        InlongClusterEntity clusterEntity = clusterMapper.selectByNameAndType(sourceEntity.getInlongClusterName(),
                ClusterType.AGENT);
        taskInfo.setParentId(clusterEntity.getId());
        List<InlongClusterNodeEntity> clusterNodes = clusterNodeMapper.selectByParentId(taskInfo.getParentId(), null);
        if (CollectionUtils.isEmpty(clusterNodes)) {
            throw new BusinessException("no dbsync cluster node exists for parentId=[" + taskInfo.getParentId() + "]");
        }
        taskInfo.setNodeIps(clusterNodes.stream().map(InlongClusterNodeEntity::getIp).collect(Collectors.toList()));

        // get the db server
        String serverName = taskInfo.getServerName();
        if (StringUtils.isBlank(serverName)) {
            LOGGER.error("db server name is null for task {}", taskInfo.getId());
            throw new BusinessException("db server name cannot be null");
        }
        MySQLDataNodeInfo mySqlNode = (MySQLDataNodeInfo) dataNodeService.get(serverName, DataNodeType.MYSQL);
        taskInfo.setDbServerInfo(DBServerInfo.builder()
                .id(mySqlNode.getId())
                .dbType(DataNodeType.MYSQL)
                .url(mySqlNode.getUrl())
                .backupUrl(mySqlNode.getBackupUrl())
                .username(mySqlNode.getUsername())
                .password(mySqlNode.getToken())
                .build());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to fulfill agent task=" + taskInfo);
        }

        return taskInfo;
    }

    /**
     * Get all valid tasks, valid status: 201, 202, 203, 204, 205, 301, 302, 303, 304, 305
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> getDbSyncTaskStatusInfo() {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> ipTaskMap = new ConcurrentHashMap<>();
        /* TDBank's behaviour:
        select dc.ip, dc.task_id, dc.status
        from data_source_config dc left join business bi on dc.business_id = bi.business_id
        left join data_schema ds on bi.schema_name = ds.schema_name
        where ds.agent_type = 'dbsync_agent' and dc.status in (201, 202, 203, 204, 301, 302, 303, 304)
         */
        List<DbSyncTaskStatus> taskStatusList = sourceMapper.selectTaskStatus(SourceType.HA_BINLOG);
        for (DbSyncTaskStatus taskStatus : taskStatusList) {
            List<InlongClusterNodeEntity> clusterNodes = clusterNodeMapper.selectByParentId(taskStatus.getId(), null);
            if (CollectionUtils.isEmpty(clusterNodes)) {
                continue;
            }
            clusterNodes.forEach(node -> {
                ConcurrentHashMap<Integer, Integer> tmpMap = ipTaskMap.getOrDefault(node.getIp(),
                        new ConcurrentHashMap<>());
                tmpMap.put(taskStatus.getId(), taskStatus.getStatus());
                ipTaskMap.put(node.getIp(), tmpMap);
            });
        }

        return ipTaskMap;
    }

    /**
     * Process the receiving and execution of DbSync after the task is issued,
     * and modify the stream source status if necessary
     */
    private void dealTaskStatus(ReportTaskRequest request) {
        List<TaskInfoBean> taskInfoList = request.getTaskInfoList();
        if (CollectionUtils.isEmpty(taskInfoList)) {
            return;
        }
        LOGGER.info("task from ip={}, report task size={}", request.getIp(), taskInfoList.size());

        for (TaskInfoBean taskInfo : taskInfoList) {
            Integer taskId = taskInfo.getId();
            StreamSourceEntity entity = sourceMapper.selectById(taskId);
            if (entity == null) {
                continue;
            }
            // if the user has modified the task after publishing, this result should be ignored
            if (!Objects.equals(entity.getVersion(), taskInfo.getVersion())) {
                LOGGER.warn("task={} version={} != newest version={}, skip it",
                        taskId, taskInfo.getVersion(), entity.getVersion());
                continue;
            }

            final Integer result = taskInfo.getResult();
            final int previousStatus = entity.getStatus();
            int nextStatus = SourceStatus.SOURCE_NORMAL.getCode();
            // only 30x status can be changed to final status
            if (previousStatus / 100 == ISSUED_STATUS) {
                if (result == TASK_SUCCESS) {
                    // agent process succeeded
                    /*if (previousStatus == SourceStatus.AGENT_ISSUED_CREATE.getCode()
                            || previousStatus == SourceStatus.AGENT_ISSUED_START.getCode()
                            || previousStatus == SourceStatus.AGENT_ISSUED_UPDATE.getCode()) {
                        // Starting 301 / Unfreezing 303 / Update been issued 305, will change to normal(101)
                        // ignore
                    } else*/
                    if (previousStatus == SourceStatus.AGENT_ISSUED_STOP.getCode()) {
                        nextStatus = SourceStatus.AGENT_FREEZE.getCode();
                    } else if (previousStatus == SourceStatus.AGENT_ISSUED_DELETE.getCode()) {
                        nextStatus = SourceStatus.AGENT_DISABLE.getCode();
                    }
                } else if (result == TASK_FAILED) {
                    // agent process failed
                    nextStatus = SourceStatus.AGENT_FAILURE.getCode();
                } else if (result == TASK_NOT_SCHEDULE) {
                    // agent scheduling fails, reset to the original 30x status and publish to agent again
                    nextStatus = previousStatus;
                }
            } else if (result == TASK_NOT_SCHEDULE && previousStatus == SourceStatus.AGENT_NORMAL.getCode()) {
                // change the normal task that failed to be scheduled to be wait created
                nextStatus = SourceStatus.AGENT_WAIT_CREATE.getCode();
            }
            // other task status of 20x will be changed to 30x when the next task is pulled
            sourceMapper.updateStatus(taskId, nextStatus, taskInfo.getMessage(), false);
            LOGGER.info("change task status from [{}] to [{}] for id [{}], dbsync result [{}]",
                    previousStatus, nextStatus, taskId, result);
        }

        LOGGER.info("success update heartbeat for ip={}", request.getIp());
    }

    /**
     * Save the heartbeat info to the table
     */
    private class SaveHeartbeatTaskRunnable implements Runnable {

        @Override
        public void run() {
            int cnt = 0;
            while (true) {
                try {
                    DbSyncHeartbeatRequest request = heartbeatQueue.poll(1, TimeUnit.SECONDS);
                    boolean empty = request == null
                            || request.getIp() == null
                            || CollectionUtils.isEmpty(request.getHeartbeats());
                    if (empty) {
                        continue;
                    }

                    String ip = request.getIp();
                    Date now = new Date();
                    List<DbSyncHeartbeatEntity> entityList = new ArrayList<>(request.getHeartbeats().size());
                    for (DbSyncHeartbeat message : request.getHeartbeats()) {
                        DbSyncHeartbeatEntity entity = new DbSyncHeartbeatEntity();
                        entity.setInstance(ip);
                        entity.setServerId(message.getServerId());
                        entity.setCurrentDb(message.getCurrentDb());
                        entity.setUrl(message.getUrl());
                        entity.setBackupUrl(message.getBackupUrl());
                        entity.setAgentStatus(message.getAgentStatus());

                        if (message.getTaskIds() != null) {
                            entity.setTaskIds(StringUtils.join(message.getTaskIds(), ","));
                        }
                        entity.setDbDumpIndex(message.getDumpIndex());
                        if (message.getDumpPosition() != null) {
                            entity.setDumpPosition(JsonUtils.toJsonString(message.getDumpPosition()));
                        }
                        if (message.getMaxLogPosition() != null) {
                            entity.setMaxLogPosition(JsonUtils.toJsonString(message.getMaxLogPosition()));
                        }
                        entity.setErrorMsg(message.getErrorMsg());
                        entity.setModifyTime(now);

                        entityList.add(entity);
                    }
                    dbSyncHeartbeatMapper.insertOrUpdateHeartbeat(entityList);

                    cnt++;
                    // get tasks in an intermediate status after processing x heartbeats
                    if (cnt > HEARTBEAT_BATCH) {
                        cnt = 0;
                        ipTaskStatusMap = getDbSyncTaskStatusInfo();
                    }
                } catch (Throwable t) {
                    LOGGER.error("dbsync heartbeat task runnable error", t);
                }
            }
        }
    }

    /**
     * Task for add fields
     */
    private class AddFieldsTaskRunnable implements Runnable {

        private static final int WAIT_SECONDS = 60 * 1000;

        @Override
        public void run() {
            while (true) {
                try {
                    processFields();
                    Thread.sleep(WAIT_SECONDS);
                } catch (Exception e) {
                    LOGGER.error("exception occurred when add fields", e);
                }
            }
        }

        @Transactional(rollbackFor = Throwable.class)
        public void processFields() {
            if (addFieldQueue.isEmpty()) {
                return;
            }
            AddFieldsRequest fieldsRequest = addFieldQueue.poll();
            Preconditions.checkNotNull(fieldsRequest, "add fields request is null from the queue");
            Integer id = fieldsRequest.getId();
            Preconditions.checkNotNull(fieldsRequest, "add fields request has no id field " + fieldsRequest);
            StreamSourceEntity dbDetailEntity = sourceMapper.selectById(id);
            Preconditions.checkNotNull(dbDetailEntity, "db detail not found by id=" + id);

            String groupId = dbDetailEntity.getInlongGroupId();
            String streamId = dbDetailEntity.getInlongStreamId();
            InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
            Preconditions.checkNotNull(groupEntity, "not found group info by groupId " + groupId);

            try {
                /*// add fields for InlongStreamField
                String defaultOperator = groupEntity.getInCharges().split(",")[0];
                streamService.addFields(groupId, streamId, fieldsRequest.getFields(), defaultOperator);

                // add fields for StreamSinkField
                sinkOperatorFactory.getAll().forEach(sinkOperator -> {
                    List<? extends BaseStorageResponse> storageList = sinkOperator.getSinkList(groupId, streamId);
                    if (CollectionUtils.isNotEmpty(storageList)) {
                        sinkOperator.addFields(groupId, streamId, fieldsRequest, defaultOperator);
                    }
                });*/
                LOGGER.info("success to add fields for dbsync, groupId={} streamId={}", groupId, streamId);
            } catch (Exception e) {
                String errMsg = String.format("failed to add fields for dbsync, groupId=%s streamId=%s ",
                        groupId, streamId);
                LOGGER.error(errMsg, e);
                throw new BusinessException(errMsg + e.getMessage());
            }
        }
    }

    /**
     * Cache the next patch tasks.
     */
    private class CacheTaskRunnable implements Runnable {

        private final String ip;
        private final String opType;
        private final List<DbSyncTaskInfo> taskInfoList;

        public CacheTaskRunnable(String ip, String opType, List<DbSyncTaskInfo> taskInfoList) {
            this.ip = ip;
            this.opType = opType;
            this.taskInfoList = taskInfoList;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<DbSyncTaskInfo> taskInfoQueue = ipTaskCacheMap.get(ip);
            if (taskInfoQueue == null) {
                ipTaskCacheMap.put(ip, new LinkedBlockingQueue<>(64));
                taskInfoQueue = ipTaskCacheMap.get(ip);
            }
            /*for (DbSyncTaskInfo taskInfo : taskInfoList) {
                changeStatusAndFillInfo(taskInfo, opType, ip);
                taskInfoQueue.add(taskInfo);
            }*/
        }
    }

}
