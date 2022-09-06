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

import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.KvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Locale;

/**
 * Default operation of inner sort config.
 */
public class AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInnerSortConfigService.class);

    @Autowired
    private ConsumptionEntityMapper consumptionEntityMapper;

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

    public String getConsumerGroup(String bid, String tid, String topic, String topoName, String mqType) {
        String consumerGroup;
        if (MQType.TUBEMQ.equals(mqType)) {
            consumerGroup = String.format(TencentConstants.SORT_TUBE_GROUP, topoName, bid);
        } else {
            consumerGroup = String.format(TencentConstants.OLD_SORT_PULSAR_GROUP, topoName, tid);
            ConsumptionEntity exists = consumptionEntityMapper.selectConsumptionExists(bid, topic, consumerGroup);
            if (exists == null) {
                consumerGroup = String.format(TencentConstants.SORT_PULSAR_GROUP, topoName, bid, tid);
            }
        }

        LOGGER.debug("success to get consumerGroup={} for bid={} tid={} topic={} topoName={}",
                consumerGroup, bid, tid, topic, topoName);
        return consumerGroup;
    }

    /**
     * Get deserializationinfo object information
     */
    public DeserializationInfo getDeserializationInfo(InlongStreamInfo streamInfo) {
        String dataType = streamInfo.getDataType();
        Character escape = null;
        DeserializationInfo deserializationInfo;
        if (streamInfo.getDataEscapeChar() != null) {
            escape = streamInfo.getDataEscapeChar().charAt(0);
        }
        // Must be a field separator in the data stream
        char separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
        if (TencentConstants.DATA_TYPE_INLONG_CSV.equalsIgnoreCase(dataType)) {
            // Do you want to delete the first separator? The default is false
            deserializationInfo = new TDMsgCsvDeserializationInfo(streamInfo.getInlongStreamId(), separator, escape,
                    false);
        } else if (TencentConstants.DATA_TYPE_INLONG_KV.equalsIgnoreCase(dataType)) {
            // KV pair separator, which must be the field separator in the data flow
            // TODO User configuration shall prevail
            char kvSeparator = '&';
            // Row separator, which must be a field separator in the data flow
            Character lineSeparator = null;
            deserializationInfo = new TDMsgKvDeserializationInfo(streamInfo.getInlongStreamId(), separator,
                    kvSeparator,
                    escape, lineSeparator);
        } else if (TencentConstants.DATA_TYPE_CSV.equalsIgnoreCase(dataType)) {
            deserializationInfo = new CsvDeserializationInfo(separator, escape);
        } else if (TencentConstants.DATA_TYPE_KV.equalsIgnoreCase(dataType)) {
            deserializationInfo = new KvDeserializationInfo(separator, escape);
        } else {
            throw new IllegalArgumentException("can not support sink data type:" + dataType);
        }
        return deserializationInfo;
    }

}
