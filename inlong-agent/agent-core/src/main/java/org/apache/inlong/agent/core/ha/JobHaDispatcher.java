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

import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import java.util.List;
import java.util.Set;

public interface JobHaDispatcher {

    /**
     * add job
     */
    void addJob(DbSyncTaskInfo taskConf);

    /**
     * update job
     */
    void updateJob(DbSyncTaskInfo taskConf);

    /**
     * start job but it may be not running
     */
    void startJob(DbSyncTaskInfo taskConf);

    /**
     * start job but it may be not running
     */
    List<DBSyncJob> startAllTasksOnce(List<DbSyncTaskInfo> taskConf);

    /**
     * stop job
     */
    void stopJob(String syncId, Integer taskId, DbSyncTaskInfo taskConf);

    /**
     * delete job
     *
     * @param syncId
     * @param taskId
     * @param taskConfFromTdm
     */
    void deleteJob(String syncId, Integer taskId, DbSyncTaskInfo taskConfFromTdm);

    /**
     * stop running job
     */
    void removeLocalRunJob(String syncId);

    /**
     * start running when it is started
     */
    void updateRunJobInfo(String syncId, JobRunNodeInfo jobRunNodeInfo);

    void updateJobCoordinator(String clusterId, String changeCoordinatorPath ,boolean isRemoved);

    void updateZkStats(String clusterId, String syncId, boolean isConnected);

    Set<JobHaInfo> getHaJobInfList();

    List<DbSyncTaskInfo> getErrorTaskConfInfList();

    List<DbSyncTaskInfo> getCorrectTaskConfInfList();

    List<DbSyncTaskInfo> getExceedTaskInfList();

    boolean updateSyncIdList(Integer clusterId, Integer syncIdListVersion, String zkServer, List<String> syncIdList,
            boolean needCheckClusterIdOrServerIdListVersion);

    void updatePosition(String syncId, String position);

    Integer getClusterId();

    Integer getSyncIdListVersion();

    Set<String> getNeedToRunSyncIdSet();

    void updateIsUpdating();

    boolean isDbsyncUpdating();

    boolean isCoordinator();

    String getZkUrl();

    ConfigDelegate getZkConfigDelegate();

    int getMaxSyncIdsThreshold();
}
