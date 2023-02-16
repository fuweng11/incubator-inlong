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

package org.apache.inlong.manager.dao.mapper.tencent;

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.dao.entity.tencent.DbSyncHeartbeatEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DbSyncHeartbeatEntityMapper {

    int insert(DbSyncHeartbeatEntity record);

    DbSyncHeartbeatEntity selectByPrimaryKey(Integer id);

    /**
     * Query the latest heartbeat info
     */
    DbSyncHeartbeatEntity getHeartbeat(@Param("taskId") Integer taskId, @Param("serverName") String serverName);

    /**
     * Query the latest heartbeat info of the specified server.
     */
    DbSyncHeartbeatEntity getHeartbeatByServerName(@Param("serverName") String serverName);

    int updateByPrimaryKey(DbSyncHeartbeatEntity record);

    /**
     * Update the heartbeat of DbSync, insert or replace it according to the primary key (instance+server_id+url)
     *
     * The db url corresponding to the server may be modified, so we need to save the heartbeat of different db urls
     * under the same server in history
     */
    void insertOrUpdateHeartbeat(@Param("entityList") List<DbSyncHeartbeatEntity> entityList);

    int deleteByPrimaryKey(Integer id);

}