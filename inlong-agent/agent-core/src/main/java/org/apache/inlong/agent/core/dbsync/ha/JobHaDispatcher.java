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

import org.apache.inlong.agent.core.dbsync.DBSyncJob;
import org.apache.inlong.agent.core.dbsync.ha.zk.ConfigDelegate;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import java.util.List;
import java.util.Set;

public interface JobHaDispatcher extends AutoCloseable {

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
    void stopJob(String dbJobId, Integer taskId, DbSyncTaskInfo taskConf);

    /**
     * delete job
     *
     * @param dbJobId
     * @param taskId
     * @param taskConfFromTdm
     */
    void deleteJob(String dbJobId, Integer taskId, DbSyncTaskInfo taskConfFromTdm);

    /**
     * stop running job
     */
    void removeLocalRunJob(String dbJobId);

    /**
     * start running when it is started
     */
    void updateRunJobInfo(String dbJobId, JobRunNodeInfo jobRunNodeInfo);

    void updateJobCoordinator(String clusterId, String changeCoordinatorPath, boolean isRemoved);

    void updateZkStats(String clusterId, String dbJobId, boolean isConnected);

    Set<JobHaInfo> getHaJobInfList();

    List<DbSyncTaskInfo> getErrorTaskConfInfList();

    List<DbSyncTaskInfo> getCorrectTaskConfInfList();

    List<DbSyncTaskInfo> getExceedTaskInfList();

    boolean updateDbJobIdList(Integer clusterId, Integer dbJobIdListVersion, String zkServer,
            List<String> dbJobIdList,
            boolean needCheckClusterIdOrServerIdListVersion);

    void updatePosition(String dbJobId, String position);

    boolean checkNodeRegisterStatus(Integer clusterId, String zkServer);

    Integer getClusterId();

    Integer getDbJobIdListVersion();

    Set<String> getNeedToRunDbJobIdSet();

    void updateIsUpdating();

    boolean isDbsyncUpdating();

    boolean isCoordinator();

    String getZkUrl();

    ConfigDelegate getZkConfigDelegate();

    int getMaxDbJobsThreshold();

    String getJobCoordinatorIp();
}
