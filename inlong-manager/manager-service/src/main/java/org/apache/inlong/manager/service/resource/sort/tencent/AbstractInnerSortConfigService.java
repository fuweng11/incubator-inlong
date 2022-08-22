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

package org.apache.inlong.manager.service.resource.sort.tencent;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Default operation of inner sort config.
 */
public class AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInnerSortConfigService.class);

    public String getZkRoot(String mqType, ZkClusterDTO zkClusterDTO) {
        Preconditions.checkNotNull(mqType, "mq type cannot be null");
        Preconditions.checkNotNull(zkClusterDTO, "zookeeper cluster cannot be null");

        String zkRoot;
        mqType = mqType.toUpperCase(Locale.ROOT);
        switch (mqType) {
            case MQType.TUBEMQ:
                zkRoot = getZkRootForTube(zkClusterDTO);
                break;
            case MQType.PULSAR:
                zkRoot = getZkRootForPulsar(zkClusterDTO);
                break;
            default:
                throw new BusinessException(ErrorCodeEnum.MQ_TYPE_NOT_SUPPORTED);
        }

        LOGGER.info("success to get zk root={} for mq type={} from cluster={}", zkRoot, mqType, zkClusterDTO);
        return zkRoot;
    }

    private String getZkRootForPulsar(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getPulsarRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.PULSAR_ROOT_DEFAULT;
        }
        return zkRoot;
    }

    private String getZkRootForTube(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getTubeRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.TUBEMQ_ROOT_DEFAULT;
        }
        return zkRoot;
    }
}
