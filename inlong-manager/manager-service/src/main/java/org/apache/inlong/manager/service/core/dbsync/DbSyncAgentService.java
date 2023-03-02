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

package org.apache.inlong.manager.service.core.dbsync;

import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeatRequest;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncInitInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskFullInfo;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;
import org.apache.inlong.common.pojo.agent.dbsync.InitTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.ReportTaskRequest;
import org.apache.inlong.common.pojo.agent.dbsync.RunningTaskRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.source.SourcePageRequest;
import org.apache.inlong.manager.pojo.source.dbsync.AddFieldsRequest;

/**
 * DbSync service interface
 */
public interface DbSyncAgentService {

    /**
     * Report the heartbeat
     *
     * @param request heartbeat info
     */
    Boolean heartbeat(DbSyncHeartbeatRequest request);

    /**
     * Query the latest heartbeat according to the DbSync task ID and the server name
     *
     * @apiNote At the same time, a task can only be executed by at most one IP and report the heartbeat,
     *         and one transfer machine may report the heartbeat of multiple tasks
     */
    DbSyncHeartbeat getHeartbeat(Integer id, String dataNodeName);

    /**
     * Pull the server names to be collected according to the IP + the version of the server list
     * associated with the cluster
     *
     * @param request request for init info
     * @return the initial info associated with the above IP
     */
    DbSyncInitInfo getInitInfo(InitTaskRequest request);

    /**
     * Query all non-failed + offline tasks
     *
     * @param request request to pull the task that needs to be run
     * @return full task info
     */
    DbSyncTaskFullInfo getRunningTask(RunningTaskRequest request);

    /**
     * DbSync returns results and pulls the latest task info.
     *
     * @param request The result information returned by
     * @return task information
     */
    DbSyncTaskFullInfo reportAndGetTask(ReportTaskRequest request);

    /**
     * DbSync adds fields, need re-config the InlongStream and StreamSink.
     *
     * @param request add fields
     * @return success or failure
     */
    Boolean addFields(AddFieldsRequest request);

    /**
     * Paging query dbsync task information based on conditions.
     *
     * @param request paging request.
     * @return dbsync task list
     */
    PageResult<DbSyncTaskInfo> listTask(SourcePageRequest request);

}
