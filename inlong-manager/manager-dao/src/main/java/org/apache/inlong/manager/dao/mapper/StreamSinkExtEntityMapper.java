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

package org.apache.inlong.manager.dao.mapper;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.inlong.manager.dao.entity.StreamSinkExtEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamSinkExtEntityMapper {

    int insert(StreamSinkExtEntity record);

    /**
     * Insert data in batches
     *
     * @param extList need to insert data
     */
    int insertAll(@Param("extList") List<StreamSinkExtEntity> extList);

    /**
     * Insert data in batches, update if it exists, create new if it does not exist
     *
     * @param extList need to insertOnDuplicateUpdate data
     */
    int insertOnDuplicateKeyUpdate(@Param("extList") List<StreamSinkExtEntity> extList);

    List<StreamSinkExtEntity> selectBySinkId(@Param("sinkId") Integer sinkId);

    List<StreamSinkExtEntity> selectByGroupId(@Param("groupId") String groupId);

    StreamSinkExtEntity selectByKey(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("keyName") String keyName);

    @Options(resultSetType = ResultSetType.FORWARD_ONLY, fetchSize = Integer.MIN_VALUE)
    Cursor<StreamSinkExtEntity> selectByKeyName(@Param("keyName") String keyName);

    int updateByPrimaryKey(StreamSinkExtEntity record);

    /**
     * Logically delete all extended fields based on sink id
     *
     * @return rows updated
     */
    int logicDeleteAllBySinkId(@Param("sinkId") Integer sinkId);

    int deleteById(Integer id);


    /**
     * Physically delete all stream sink ext based on inlong group ids
     *
     * @return rows deleted
     */
    int deleteByInlongGroupIds(@Param("groupIdList") List<String> groupIdList);

}
