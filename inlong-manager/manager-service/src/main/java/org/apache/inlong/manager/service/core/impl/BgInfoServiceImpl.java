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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.tencent.BgInfoEntity;
import org.apache.inlong.manager.dao.mapper.tencent.BgInfoEntityMapper;
import org.apache.inlong.manager.pojo.tencent.BgInfoResponse;
import org.apache.inlong.manager.service.core.BgInfoService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * BG information service impl
 */
@Service
public class BgInfoServiceImpl implements BgInfoService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BgInfoServiceImpl.class);

    @Autowired
    private BgInfoEntityMapper bgInfoEntityMapper;

    @Override
    public BgInfoResponse get(Integer id) {
        BgInfoEntity entity = bgInfoEntityMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("bg info not found by id={}", id);
            throw new BusinessException("bg info not found");
        }
        LOGGER.debug("success to get bg info by id={}", id);
        return this.mapFromEntity(entity);
    }

    @Override
    public BgInfoResponse get(String abbrevName) {
        BgInfoEntity entity = bgInfoEntityMapper.selectByAbbrevName(abbrevName);
        if (entity == null) {
            LOGGER.error("bg info not found by abbrev name={}", abbrevName);
            throw new BusinessException("bg info not found");
        }
        LOGGER.debug("success to get bg info by id={}", abbrevName);
        return this.mapFromEntity(entity);
    }

    @Override
    public Collection<BgInfoResponse> list() {
        return this.bgInfoEntityMapper.listAll()
                .stream()
                .map(this::mapFromEntity)
                .collect(Collectors.toList());
    }

    private BgInfoResponse mapFromEntity(BgInfoEntity entity) {
        return BgInfoResponse.builder()
                .id(entity.getId())
                .name(entity.getName())
                .abbrevName(entity.getAbbrevName())
                .usBgId(entity.getUsBgId())
                .build();
    }
}
