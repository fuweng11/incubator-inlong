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

package org.apache.inlong.manager.service.resource.sort.tencent.hive;

import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.flink.formats.common.StringFormatInfo;
import com.tencent.flink.formats.common.TimestampFormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.configuration.Constants.CompressionType;
import com.tencent.oceanus.etl.configuration.Constants.SequenceCompressionCodec;
import com.tencent.oceanus.etl.configuration.Constants.SequenceCompressionType;
import com.tencent.oceanus.etl.protocol.BuiltInFieldInfo;
import com.tencent.oceanus.etl.protocol.BuiltInFieldInfo.BuiltInField;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.KafkaClusterInfo;
import com.tencent.oceanus.etl.protocol.PulsarClusterInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.ConsistencyGuarantee;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HiveFileFormatInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.PartitionCreationStrategy;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo.THivePartitionType;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo.THiveTimePartitionInfo;
import com.tencent.oceanus.etl.protocol.source.KafkaSourceInfo;
import com.tencent.oceanus.etl.protocol.source.PulsarSourceInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterDTO;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.sort.BaseSortClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.service.resource.sink.tencent.us.UPSOperator;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.resource.sort.tencent.AbstractInnerSortConfigService;
import org.apache.inlong.manager.service.sink.tencent.sort.SortExtConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.manager.common.consts.TencentConstants.PART_ARRIVED;
import static org.apache.inlong.manager.common.consts.TencentConstants.PART_COUNT_VERIFIED;

/**
 * Inner Sort config operator, used to create a Sort config for the InlongGroup with ZK enabled.
 */
@Service
public class SortHiveConfigService extends AbstractInnerSortConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortHiveConfigService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AT_LEAST_ONCE = "AT_LEAST_ONCE";

    private static final Map<String, String> TIME_UNIT_MAP = new HashMap<>();

    private static final Map<String, String> PARTITION_TIME_FORMAT_MAP = new HashMap<>();

    private static final Map<String, TimeUnit> PARTITION_TIME_UNIT_MAP = new HashMap<>();
    /**
     * Built in fields that sort needs to process when the source data is dbsync
     */
    private static final Map<String, BuiltInField> BUILT_IN_FIELD_MAP = new HashMap<>();

    private static final Map<String, CompressionType> COMPRESSION_TYPE_MAP = new HashMap<>();

    static {
        TIME_UNIT_MAP.put("10I", "t");
        TIME_UNIT_MAP.put("15I", "q");
        TIME_UNIT_MAP.put("30I", "n");
        TIME_UNIT_MAP.put("1H", "h");
        TIME_UNIT_MAP.put("1D", "d");

        PARTITION_TIME_FORMAT_MAP.put("D", "yyyyMMdd");
        PARTITION_TIME_FORMAT_MAP.put("H", "yyyyMMddHH");
        PARTITION_TIME_FORMAT_MAP.put("I", "yyyyMMddHHmm");

        PARTITION_TIME_UNIT_MAP.put("D", TimeUnit.DAYS);
        PARTITION_TIME_UNIT_MAP.put("H", TimeUnit.HOURS);
        PARTITION_TIME_UNIT_MAP.put("I", TimeUnit.MINUTES);

        BUILT_IN_FIELD_MAP.put("db_name", BuiltInField.DBSYNC_DB_NAME);
        BUILT_IN_FIELD_MAP.put("tb_name", BuiltInField.DBSYNC_TABLE_NAME);
        BUILT_IN_FIELD_MAP.put("op_name", BuiltInField.DBSYNC_OPERATION_TYPE);
        BUILT_IN_FIELD_MAP.put("exp_time_stample", BuiltInField.DBSYNC_EXECUTE_TIME);
        BUILT_IN_FIELD_MAP.put("exp_time_stample_order", BuiltInField.DBSYNC_EXECUTE_ORDER);
        BUILT_IN_FIELD_MAP.put("tdbank_transfer_ip", BuiltInField.DBSYNC_TRANSFER_IP);
        BUILT_IN_FIELD_MAP.put("dt", BuiltInField.DATA_TIME);

        COMPRESSION_TYPE_MAP.put("none", CompressionType.NONE);
        COMPRESSION_TYPE_MAP.put("gzip", CompressionType.GZIP);
        COMPRESSION_TYPE_MAP.put("lzo", CompressionType.LZO);
    }

    @Autowired
    private StreamSinkEntityMapper sinkMapper;

    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    @Autowired
    private InlongClusterEntityMapper clusterMapper;

    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;

    @Autowired
    private UPSOperator upsOperator;

    public void buildHiveConfig(InlongGroupInfo groupInfo, List<InnerHiveFullInfo> hiveFullInfos) throws Exception {
        if (CollectionUtils.isEmpty(hiveFullInfos)) {
            return;
        }
        String groupId = groupInfo.getInlongGroupId();
        LOGGER.info("hive sort info: " + OBJECT_MAPPER.writeValueAsString(hiveFullInfos));

        List<InlongClusterEntity> zkClusters = clusterMapper.selectByKey(groupInfo.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters) || StringUtils.isBlank(zkClusters.get(0).getUrl())) {
            throw new WorkflowListenerException("sort zk cluster not found for groupId=" + groupId);
        }
        InlongClusterEntity zkCluster = zkClusters.get(0);
        ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());

        String zkUrl = zkCluster.getUrl();
        String zkRoot = getZkRoot(groupInfo.getMqType(), zkClusterDTO);
        for (InnerHiveFullInfo hiveFullInfo : hiveFullInfos) {
            String topoType = hiveFullInfo.getIsThive() == 1 ? ClusterType.SORT_THIVE : ClusterType.SORT_HIVE;
            String sortClusterName = getSortTaskName(groupInfo.getInlongGroupId(), groupInfo.getInlongClusterTag(),
                    hiveFullInfo.getSinkId(), topoType);

            List<InlongClusterEntity> sortClusters = clusterMapper.selectByKey(groupInfo.getInlongClusterTag(),
                    sortClusterName, topoType);
            if (CollectionUtils.isEmpty(sortClusters) || StringUtils.isBlank(sortClusters.get(0).getName())) {
                throw new WorkflowListenerException("sort cluster not found for groupId=" + groupId);
            }
            InlongClusterEntity sortCluster = sortClusters.get(0);

            // Backup configuration
            BaseSortClusterDTO sortClusterDTO = BaseSortClusterDTO.getFromJson(sortCluster.getExtParams());
            SortExtConfig sortExtConfig = new SortExtConfig();
            sortExtConfig.setBackupDataPath(sortClusterDTO.getBackupDataPath());
            sortExtConfig.setBackupHadoopProxyUser(sortClusterDTO.getBackupHadoopProxyUser());
            // get and save hdfs location
            upsOperator.getAndSaveLocation(hiveFullInfo);
            LOGGER.info("begin to push hive sort config to zkUrl={}, hiveTopo={}", zkUrl, sortClusterName);
            DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, hiveFullInfo, sortClusterName, sortExtConfig);
            // Update / add data under dataflow on ZK
            ZkTools.updateDataFlowInfo(flowInfo, sortClusterName, flowInfo.getId(), zkUrl, zkRoot);
            // Add data under clusters on ZK
            ZkTools.addDataFlowToCluster(sortClusterName, flowInfo.getId(), zkUrl, zkRoot);

            LOGGER.info("success to push hive sort config {}", OBJECT_MAPPER.writeValueAsString(flowInfo));
        }
    }

    /**
     * Get DataFlowInfo for Sort
     */
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, InnerHiveFullInfo hiveFullInfo,
            String sortClusterName, SortExtConfig sortExtConfig) throws Exception {
        // Get fields from the source fields saved in the data store:
        // the number and order of the source fields must be the same as the target fields
        String groupId = hiveFullInfo.getInlongGroupId();
        String streamId = hiveFullInfo.getInlongStreamId();
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(hiveFullInfo.getSinkId());

        if (fieldList == null || fieldList.size() == 0) {
            throw new WorkflowListenerException("fields is null for group id=" + groupId + ", stream id=" + streamId);
        }

        SourceInfo sourceInfo = this.getSourceInfo(groupInfo, hiveFullInfo, fieldList, sortClusterName);
        com.tencent.oceanus.etl.protocol.sink.SinkInfo sinkInfo = getSinkInfo(hiveFullInfo, fieldList, sortExtConfig);

        // Dynamic configuration information,
        // which can be used to specify optional parameter information of source or sink
        // After that, source.tdbank.bid, source.tdbank.tid will be dropped
        // Just stay source.inlong.groupId, source.inlong.streamId
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("source.tdbank.bid", groupInfo.getInlongGroupId());
        properties.put("source.tdbank.tid", hiveFullInfo.getInlongStreamId());
        properties.put("source.inlongGroupId", groupInfo.getInlongGroupId());
        properties.put("source.inlongStreamId", hiveFullInfo.getInlongStreamId());
        String flowId = hiveFullInfo.getSinkId().toString();
        DataFlowInfo flowInfo = new DataFlowInfo(flowId, sourceInfo, sinkInfo, properties);
        LOGGER.info("hive data flow info: " + OBJECT_MAPPER.writeValueAsString(flowInfo));

        return flowInfo;
    }

    /**
     * Get the sink information of sort
     *
     * @apiNote The fields should be in the same order as the fields in the source.
     *         The extra partition fields and dbsync meta fields should be placed last
     */
    private com.tencent.oceanus.etl.protocol.sink.SinkInfo getSinkInfo(InnerHiveFullInfo hiveFullInfo,
            List<StreamSinkFieldEntity> fieldList, SortExtConfig sortExtConfig) {
        if (hiveFullInfo.getHiveAddress() == null) {
            throw new BusinessException("hive server url cannot be empty");
        }

        // Must be the field separator in hive, and the default is text file
        Character separator = (char) Integer.parseInt(hiveFullInfo.getTargetSeparator());
        HiveFileFormatInfo fileFormat;
        String format = hiveFullInfo.getFileFormat();

        // when the file format is text, set all fields to string.
        boolean isTextFormat = false;
        // Currently, sort does not support FILE_FORMAT_RC
        if (TencentConstants.FILE_FORMAT_ORC.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.OrcFileFormatInfo();
        } else if (TencentConstants.FILE_FORMAT_SEQUENCE.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.SequenceFileFormatInfo(separator,
                    SequenceCompressionCodec.DEFAULT, SequenceCompressionType.NONE);
        } else if (TencentConstants.FILE_FORMAT_MY_SEQUENCE.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.MySequenceFileFormatInfo(separator,
                    SequenceCompressionCodec.DEFAULT, SequenceCompressionType.NONE);
        } else if (TencentConstants.FILE_FORMAT_PARQUET.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.ParquetFileFormatInfo();
        } else {
            isTextFormat = true;
            CompressionType compressionType = COMPRESSION_TYPE_MAP.get(hiveFullInfo.getCompressionType());
            if (!Objects.isNull(compressionType)) {
                fileFormat = new HiveSinkInfo.TextFileFormatInfo(separator, compressionType,
                        new HiveSinkInfo.TextFileFormatInfo.FixedReplaceEscapeMode(String.valueOf(separator)));
            } else {
                fileFormat = new HiveSinkInfo.TextFileFormatInfo(separator);
            }
        }
        sortExtConfig.setFormatInfo(fileFormat);

        String createStrategy = hiveFullInfo.getPartitionCreationStrategy();
        PartitionCreationStrategy creationStrategy = PartitionCreationStrategy.COMPLETED;
        if (PART_ARRIVED.equals(createStrategy)) {
            creationStrategy = PartitionCreationStrategy.ARRIVED;
        } else if (PART_COUNT_VERIFIED.equalsIgnoreCase(createStrategy)) {
            // TODO If the data quantity passes the verification,
            // the [absolute value error] and [relative value error] shall be pushed
            creationStrategy = PartitionCreationStrategy.AGENT_COUNT_VERIFIED;
        } /*
           * else if (PART_DISTINCT_VERIFIED.equalsIgnoreCase(createStrategy)) { // The data deduplication verification
           * is passed. It is only used to create us tasks. Sort is still completed for partitions creationStrategy =
           * PartitionCreationStrategy.COMPLETED; }
           */
        sortExtConfig.setCreationStrategy(creationStrategy);

        // Data consistency assurance
        ConsistencyGuarantee consistency = ConsistencyGuarantee.EXACTLY_ONCE;
        String consistencyStr = hiveFullInfo.getDataConsistency();
        if (AT_LEAST_ONCE.equals(consistencyStr)) {
            consistency = ConsistencyGuarantee.AT_LEAST_ONCE;
        }
        sortExtConfig.setConsistency(consistency);

        // Get the sink field. If there is no partition field in the source field, add the partition field to the last
        List<FieldInfo> fieldInfoList = getSinkFields(fieldList, hiveFullInfo.getPrimaryPartition(), isTextFormat);

        com.tencent.oceanus.etl.protocol.sink.SinkInfo sinkInfo;
        if (hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE) {
            if (StringUtils.isBlank(hiveFullInfo.getUsTaskId())) {
                throw new BusinessException(String.format("us task id cannot be empty for bid=%s, tid=%s",
                        hiveFullInfo.getInlongGroupId(), hiveFullInfo.getInlongStreamId()));
            }
            sinkInfo = getTHiveSinkInfo(hiveFullInfo, hiveFullInfo.getLocation(), fieldInfoList, sortExtConfig);
        } else {
            sinkInfo = getHiveSinkInfo(hiveFullInfo, hiveFullInfo.getLocation(), fieldInfoList, sortExtConfig);
        }
        return sinkInfo;
    }

    /**
     * Get hive sink information
     */
    private com.tencent.oceanus.etl.protocol.sink.SinkInfo getHiveSinkInfo(InnerHiveFullInfo hiveFullInfo,
            String dataPath, List<FieldInfo> fieldInfoList, SortExtConfig sortExtConfig) {

        List<HiveSinkInfo.HivePartitionInfo> partitionList = new ArrayList<>();

        // Level 1 partition field, the type in sink must be hivetimepartitioninfo
        String primary = hiveFullInfo.getPrimaryPartition();

        if (StringUtils.isNotEmpty(primary)) {
            // Hive is divided by day, hour and minute
            long interval = hiveFullInfo.getPartitionInterval();
            String unit = hiveFullInfo.getPartitionUnit();
            HiveTimePartitionInfo timePartitionInfo = new HiveTimePartitionInfo(
                    primary,
                    PARTITION_TIME_FORMAT_MAP.get(unit),
                    interval,
                    PARTITION_TIME_UNIT_MAP.get(unit));
            partitionList.add(timePartitionInfo);
        }
        // Level 2 partition field, the type in sink is temporarily encapsulated into hivefieldpartitioninfo,
        // which is set according to the type of the field itself
        if (StringUtils.isNotEmpty(hiveFullInfo.getSecondaryPartition())) {
            partitionList.add(new HiveSinkInfo.HiveFieldPartitionInfo(hiveFullInfo.getSecondaryPartition()));
        }

        // Hive's JDBC connection
        String serverUrl = hiveFullInfo.getHiveAddress();
        if (!serverUrl.startsWith("jdbc")) {
            serverUrl = "jdbc:hive2://" + serverUrl;
        }
        String omsServerUrl = hiveFullInfo.getOmsAddress();
        if (StringUtils.isNotBlank(omsServerUrl)) {
            serverUrl = omsServerUrl;
        }

        // The virtual user, namely Hadoop proxyuser, is used to write HDFS
        String user = hiveFullInfo.getVirtualUser();
        if (StringUtils.isBlank(user)) {
            user = hiveFullInfo.getCreator();
        }

        if (StringUtils.isBlank(sortExtConfig.getBackupDataPath())) {
            sortExtConfig.setBackupDataPath(dataPath);
        }
        if (StringUtils.isBlank(sortExtConfig.getBackupHadoopProxyUser())) {
            sortExtConfig.setBackupHadoopProxyUser(user);
        }
        return new HiveSinkInfo(fieldInfoList.toArray(new FieldInfo[0]),
                serverUrl,
                hiveFullInfo.getDbName(), hiveFullInfo.getTableName(),
                hiveFullInfo.getUsername(), hiveFullInfo.getPassword(),
                dataPath, sortExtConfig.getBackupDataPath(),
                user, sortExtConfig.getBackupHadoopProxyUser(),
                3,
                sortExtConfig.getCreationStrategy(),
                partitionList.toArray(new HivePartitionInfo[0]),
                sortExtConfig.getFormatInfo(),
                sortExtConfig.getConsistency());
    }

    /**
     * Get thive sink information
     *
     * @apiNote Thive does not support secondary partition at present
     */
    private com.tencent.oceanus.etl.protocol.sink.SinkInfo getTHiveSinkInfo(InnerHiveFullInfo hiveFullInfo,
            String dataPath, List<FieldInfo> fieldInfoList, SortExtConfig sortExtConfig) {

        // Only thive has partition types, such as range / list
        String partType = hiveFullInfo.getPartitionType().toUpperCase();

        // Thive's first level partition field, compatible with the old version,
        // must be tdbank_ imp_ Date, the type in sink is thivetimepartitioninfo
        List<THiveSinkInfo.THivePartitionInfo> partitionInfos = new ArrayList<>();
        if (StringUtils.isNotEmpty(hiveFullInfo.getPrimaryPartition())) {
            long interval = hiveFullInfo.getPartitionInterval();
            String unit = hiveFullInfo.getPartitionUnit();
            THiveTimePartitionInfo timePartitionInfo = new THiveTimePartitionInfo(
                    hiveFullInfo.getPrimaryPartition(),
                    THivePartitionType.valueOf(partType),
                    PARTITION_TIME_FORMAT_MAP.get(unit),
                    interval,
                    PARTITION_TIME_UNIT_MAP.get(unit));

            partitionInfos.add(timePartitionInfo);
        }

        // Thive's JDBC connection
        String serverUrl = hiveFullInfo.getHiveAddress();
        if (!serverUrl.startsWith("jdbc")) {
            serverUrl = "jdbc:hive://" + serverUrl;
        }
        String omsServerUrl = hiveFullInfo.getOmsAddress();
        if (StringUtils.isNotBlank(omsServerUrl)) {
            serverUrl = omsServerUrl;
        }

        // info.getUsername(); // Create partitions through the thive JDBC server link
        // info.getCreator() - tdwUsername // Go to TDW to query whether the partition exists
        String hadoopProxyUser = "tdwadmin"; // Sort write HDFS use
        if (StringUtils.isBlank(sortExtConfig.getBackupDataPath())) {
            sortExtConfig.setBackupDataPath(dataPath);
        }
        if (StringUtils.isBlank(sortExtConfig.getBackupHadoopProxyUser())) {
            sortExtConfig.setBackupHadoopProxyUser(hadoopProxyUser);
        }

        return new THiveSinkInfo(fieldInfoList.toArray(new FieldInfo[0]), serverUrl,
                hiveFullInfo.getDbName(), hiveFullInfo.getTableName(),
                hiveFullInfo.getUsername(), hiveFullInfo.getPassword(),
                dataPath, hadoopProxyUser,
                sortExtConfig.getCreationStrategy(),
                partitionInfos.toArray(new THiveSinkInfo.THivePartitionInfo[0]),
                sortExtConfig.getFormatInfo(),
                hiveFullInfo.getAppGroupName(),
                hiveFullInfo.getCreator(),
                sortExtConfig.getConsistency(),
                hiveFullInfo.getUsTaskId(),
                sortExtConfig.getBackupDataPath(),
                sortExtConfig.getBackupHadoopProxyUser(), 3);
    }

    /**
     * Get the sink field information.
     * If there is a partition field in the common field, use it. Otherwise, add the partition field to the last
     */
    private List<FieldInfo> getSinkFields(
            List<StreamSinkFieldEntity> fieldList,
            String partitionField,
            boolean isTextFormat) {
        boolean duplicate = false;
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            String fieldName = field.getFieldName();
            if (fieldName.equals(partitionField)) {
                duplicate = true;
            }
            FormatInfo formatInfo = isTextFormat? new StringFormatInfo() : SortFieldFormatUtils.convertFieldFormat(
                    field.getFieldType().toLowerCase(), field.getFieldFormat());
            //In order to ensure the successful deserialization of etl2.0, we set source type of each fields to string
            FieldInfo fieldInfo = new FieldInfo(fieldName, formatInfo);
            fieldInfoList.add(fieldInfo);
        }

        // There is no partition field in the common field. Add the partition field to the last
        if (!duplicate && StringUtils.isNotEmpty(partitionField)) {
            FieldInfo fieldInfo = new FieldInfo(partitionField, new TimestampFormatInfo("MILLIS"));
            fieldInfoList.add(0, fieldInfo);
        }
        return fieldInfoList;
    }

    /**
     * Get source info
     *
     * @apiNote The fields in the list should be in the same order as
     *         the other fields in the sink except the partition field - there is no partition field in the source
     */
    private SourceInfo getSourceInfo(InlongGroupInfo groupInfo, InnerHiveFullInfo hiveFullInfo,
            List<StreamSinkFieldEntity> fieldList, String sortClusterName) {
        String streamId = hiveFullInfo.getInlongStreamId();
        InlongStreamEntity stream = streamEntityMapper.selectByIdentifier(groupInfo.getInlongGroupId(), streamId);

        DeserializationInfo deserializationInfo = getDeserializationInfo(stream);

        // Source fields are to be obtained from the source fields saved in the data store:
        // the number and order of source fields must be the same as the target fields
        SourceInfo sourceInfo = null;
        // Get the source field. If there is no partition field in the source, add the partition field to the last
        List<FieldInfo> sourceFields = getSourceFields(fieldList, hiveFullInfo.getPrimaryPartition(),
                "TextFile".equalsIgnoreCase(hiveFullInfo.getFileFormat()));

        String groupId = groupInfo.getInlongGroupId();
        String mqType = groupInfo.getMqType();
        String clusterTag = groupInfo.getInlongClusterTag();
        if (MQType.TUBEMQ.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> tubeClusters = clusterMapper.selectByKey(clusterTag, null, MQType.TUBEMQ);
            if (CollectionUtils.isEmpty(tubeClusters)) {
                throw new WorkflowListenerException("tube cluster not found for groupId=" + groupId);
            }
            InlongClusterEntity tubeCluster = tubeClusters.get(0);
            Preconditions.expectNotNull(tubeCluster, "tube cluster not found for bid=" + groupId);
            Integer tubeId = tubeCluster.getId();
            String masterAddress = tubeCluster.getUrl();
            Preconditions.expectNotNull(masterAddress, "tube cluster [" + tubeId + "] not contains masterAddress");

            String topic = groupInfo.getMqResource();
            String consumerGroup = getConsumerGroup(groupInfo, topic, sortClusterName, hiveFullInfo.getSinkId());
            sourceInfo = new TubeSourceInfo(topic, masterAddress, consumerGroup,
                    deserializationInfo, sourceFields.toArray(new FieldInfo[0]));
        } else if (MQType.PULSAR.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> pulsarClusters = clusterMapper.selectByKey(clusterTag, null, MQType.PULSAR);
            if (CollectionUtils.isEmpty(pulsarClusters)) {
                throw new WorkflowListenerException("pulsar cluster not found for groupId=" + groupId);
            }

            List<PulsarClusterInfo> pulsarClusterInfos = new ArrayList<>();
            pulsarClusters.forEach(pulsarCluster -> {
                // Multiple adminUrls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(pulsarCluster.getExtParams());
                String adminUrl = pulsarClusterDTO.getAdminUrl();
                String serviceUrl = pulsarCluster.getUrl();
                pulsarClusterInfos.add(new PulsarClusterInfo(adminUrl, serviceUrl, pulsarCluster.getName(), null, null));
            });
            InlongClusterEntity pulsarCluster = pulsarClusters.get(0);
            // Multiple adminUrls should be configured for pulsar,
            // otherwise all requests will be sent to the same broker
            PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(pulsarCluster.getExtParams());

            String tenant = pulsarClusterDTO.getTenant() == null ? InlongConstants.DEFAULT_PULSAR_TENANT
                    : pulsarClusterDTO.getTenant();
            String namespace = groupInfo.getMqResource();
            String topic = hiveFullInfo.getMqResourceObj();
            // Full path of topic in pulsar
            String fullTopic = "persistent://" + tenant + "/" + namespace + "/" + topic;
            try {
                // Ensure compatibility of old data: if the old subscription exists, use the old one;
                // otherwise, create the subscription according to the new rule
                String subscription = getConsumerGroup(groupInfo, topic, sortClusterName, hiveFullInfo.getSinkId());
                sourceInfo = new PulsarSourceInfo(null, null, fullTopic, subscription,
                        deserializationInfo, sourceFields.toArray(new FieldInfo[0]),
                        pulsarClusterInfos.toArray(new PulsarClusterInfo[0]), null);
            } catch (Exception e) {
                LOGGER.error("get pulsar information failed", e);
                throw new WorkflowListenerException("get pulsar admin failed, reason: " + e.getMessage());
            }
        } else if (MQType.KAFKA.equalsIgnoreCase(mqType)) {
            List<InlongClusterEntity> kafkaClusters = clusterMapper.selectByKey(
                    groupInfo.getInlongClusterTag(), null, MQType.KAFKA);
            if (CollectionUtils.isEmpty(kafkaClusters)) {
                throw new WorkflowListenerException("kafka cluster not found for groupId=" + groupId);
            }
            List<KafkaClusterInfo> kafkaClusterInfos = new ArrayList<>();
            kafkaClusters.forEach(kafkaCluster -> {
                // Multiple adminurls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                KafkaClusterDTO kafkaClusterDTO = KafkaClusterDTO.getFromJson(kafkaCluster.getExtParams());
                String bootstrapServers = kafkaClusterDTO.getBootstrapServers();
                kafkaClusterInfos.add(new KafkaClusterInfo(bootstrapServers));
            });
            try {
                String topic = stream.getMqResource();
                if (topic.equals(streamId)) {
                    // the default mq resource (stream id) is not sufficient to discriminate different kafka topics
                    topic = String.format(org.apache.inlong.common.constant.Constants.DEFAULT_KAFKA_TOPIC_FORMAT,
                            groupInfo.getMqResource(), stream.getMqResource());
                }
                deserializationInfo = getDeserializationInfo(stream);
                sourceInfo = new KafkaSourceInfo(kafkaClusterInfos.toArray(new KafkaClusterInfo[0]), topic, groupId,
                        deserializationInfo,
                        fieldList.stream().map(f -> {
                            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                                    f.getSourceFieldType().toLowerCase());
                            return new FieldInfo(f.getSourceFieldType(), formatInfo);
                        }).toArray(FieldInfo[]::new));
            } catch (Exception e) {
                LOGGER.error("get kafka information failed", e);
                throw new WorkflowListenerException("get kafka admin failed, reason: " + e.getMessage());
            }
        }

        return sourceInfo;
    }

    /**
     * Get the source field information, pay attention to encapsulating the built-in field,
     * and generally handle the same field as the partition field
     *
     * @see <a href="https://iwiki.woa.com/pages/viewpage.action?pageId=989893490">Field info protocol</a>
     */
    private List<FieldInfo> getSourceFields(
            List<StreamSinkFieldEntity> fieldList,
            String partitionField,
            boolean isTextFormat) {
        boolean duplicate = false;
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            FormatInfo formatInfo = isTextFormat? new StringFormatInfo() : SortFieldFormatUtils.convertFieldFormat(
                   field.getSourceFieldType().toLowerCase());
            //In order to ensure the successful deserialization of etl2.0, we set source type of each fields to string
            String fieldName = field.getSourceFieldName();

            FieldInfo fieldInfo;
            // Determine whether it is a normal field or a built-in field.
            // If the field name is the same as the partition field, set this field as a normal field
            BuiltInField builtInField = BUILT_IN_FIELD_MAP.get(fieldName);
            if (builtInField == null) {
                fieldInfo = new FieldInfo(fieldName, formatInfo);
            } else if (fieldName.equals(partitionField)) {
                duplicate = true;
                fieldInfo = new FieldInfo(fieldName, formatInfo);
            } else {
                fieldInfo = new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
            }
            fieldInfoList.add(fieldInfo);
        }

        // If there is no partition field in the field list, the partition field is appended to the front
        // @see Field order in hivetableoperator # gettableinfo
        if (!duplicate && StringUtils.isNotBlank(partitionField)) {
            fieldInfoList.add(0, new BuiltInFieldInfo(partitionField, new TimestampFormatInfo("MILLIS"),
                    BuiltInField.DATA_TIME));
        }

        return fieldInfoList;
    }

}
