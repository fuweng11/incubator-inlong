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

import org.apache.inlong.manager.dao.entity.tencent.FieldChangeLogEntity;
import org.apache.inlong.manager.dao.mapper.tencent.FieldChangeLogEntityMapper;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.source.dbsync.FieldChangLogRequest;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FieldChangeLogServiceImpl implements FieldChangeLogService {

    @Autowired
    private FieldChangeLogEntityMapper fieldChangeLogEntityMapper;

    @Override
    public PageResult<FieldChangeLogEntity> list(FieldChangLogRequest request) {
        PageHelper.startPage(0, 10);
        // Support min agg at now
        Page<FieldChangeLogEntity> entityPage =
                (Page<FieldChangeLogEntity>) fieldChangeLogEntityMapper.selectByCondition(
                        request);
        PageResult<FieldChangeLogEntity> pageResult = new PageResult<>(entityPage, entityPage.getTotal(),
                entityPage.getPageNum(), entityPage.getPageSize());
        return pageResult;
    }
}