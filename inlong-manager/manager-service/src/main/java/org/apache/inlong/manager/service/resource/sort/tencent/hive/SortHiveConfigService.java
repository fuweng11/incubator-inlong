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

import com.google.gson.Gson;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.flink.formats.common.TimestampFormatInfo;
import com.tencent.oceanus.etl.ZkTools;
import com.tencent.oceanus.etl.protocol.BuiltInFieldInfo;
import com.tencent.oceanus.etl.protocol.BuiltInFieldInfo.BuiltInField;
import com.tencent.oceanus.etl.protocol.DataFlowInfo;
import com.tencent.oceanus.etl.protocol.FieldInfo;
import com.tencent.oceanus.etl.protocol.deserialization.CsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.DeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgCsvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgDBSyncDeserializationInfo;
import com.tencent.oceanus.etl.protocol.deserialization.TDMsgKvDeserializationInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.ConsistencyGuarantee;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HiveFileFormatInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import com.tencent.oceanus.etl.protocol.sink.HiveSinkInfo.PartitionCreationStrategy;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo.THivePartitionType;
import com.tencent.oceanus.etl.protocol.sink.THiveSinkInfo.THiveTimePartitionInfo;
import com.tencent.oceanus.etl.protocol.source.PulsarSourceInfo;
import com.tencent.oceanus.etl.protocol.source.SourceInfo;
import com.tencent.oceanus.etl.protocol.source.TubeSourceInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.consts.TencentConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tencent.zk.ZkClusterDTO;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterDTO;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveFullInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.resource.sort.SortFieldFormatUtils;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.consts.TencentConstants.PART_ARRIVED;
import static org.apache.inlong.manager.common.consts.TencentConstants.PART_COUNT_VERIFIED;

/**
 * Inner Sort config operator, used to create a Sort config for the InlongGroup with ZK enabled.
 */
@Service
public class SortHiveConfigService {

    public static final String STORAGE_HIVE = "HIVE";
    public static final String STORAGE_THIVE = "THIVE";
    private static final Logger LOGGER = LoggerFactory.getLogger(SortHiveConfigService.class);
    private static final Gson GSON = new Gson();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AT_LEAST_ONCE = "AT_LEAST_ONCE";

    private static final Map<String, String> TIME_UNIT_MAP = new HashMap<>();

    private static final Map<String, String> PARTITION_TIME_FORMAT_MAP = new HashMap<>();

    private static final Map<String, TimeUnit> PARTITION_TIME_UNIT_MAP = new HashMap<>();
    /**
     * Built in fields that sort needs to process when the source data is dbsync
     */
    private static final Map<String, BuiltInField> BUILT_IN_FIELD_MAP = new HashMap<>();

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
    }

    @Value("${cluster.hive.topo:etl_2_for_test}")
    private String clusterHiveTopo;

    @Autowired
    private StreamSinkFieldEntityMapper sinkFieldMapper;

    @Autowired
    private InlongClusterEntityMapper inlongClusterEntityMapper;

    @Autowired
    private ConsumptionEntityMapper consumptionEntityMapper;

    @Autowired
    private InlongStreamService inlongStreamService;

    public void buildHiveConfig(InlongGroupInfo groupInfo, List<InnerHiveFullInfo> hiveFullInfos)
            throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        LOGGER.info("hive sort info: " + OBJECT_MAPPER.writeValueAsString(hiveFullInfos));

        List<InlongClusterEntity> zkClusters = inlongClusterEntityMapper.selectByKey(groupInfo.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters) || StringUtils.isBlank(zkClusters.get(0).getUrl())) {
            throw new WorkflowListenerException("sort zk cluster not found for groupId=" + groupId);
        }
        InlongClusterEntity zkCluster = zkClusters.get(0);
        ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());

        String zkUrl = zkCluster.getUrl();
        String zkRoot = getZkRoot(groupInfo.getMqType(), zkClusterDTO);
        String topoName = clusterHiveTopo;
        if (topoName == null || StringUtils.isBlank(topoName)) {
            throw new WorkflowListenerException("hive topo cluster not found for groupId=" + groupId);
        }
        for (InnerHiveFullInfo hiveFullInfo : hiveFullInfos) {
            LOGGER.info("begin to push hive sort config to zkUrl={}, hiveTopo={}", zkUrl, topoName);
            DataFlowInfo flowInfo = getDataFlowInfo(groupInfo, hiveFullInfo, topoName);
            // Update / add data under dataflow on ZK
            ZkTools.updateDataFlowInfo(flowInfo, topoName, flowInfo.getId(), zkUrl, zkRoot);
            // Add data under clusters on ZK
            ZkTools.addDataFlowToCluster(topoName, flowInfo.getId(), zkUrl, zkRoot);

            LOGGER.info("success to push hive sort config {}", OBJECT_MAPPER.writeValueAsString(flowInfo));
        }
    }

    public String getZkRoot(String mqType, ZkClusterDTO zkClusterDTO) {
        Preconditions.checkNotNull(mqType, "mq type cannot be null");
        Preconditions.checkNotNull(zkClusterDTO, "zookeeper cluster cannot be null");

        LOGGER.info("begin to get zk root for my type={}", mqType);

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

        LOGGER.info("success, zk root={} from cluster={}", zkRoot, zkClusterDTO);
        return zkRoot;
    }

    /**
     * Get DataFlowInfo for Sort
     */
    private DataFlowInfo getDataFlowInfo(InlongGroupInfo groupInfo, InnerHiveFullInfo hiveFullInfo, String topoName)
            throws Exception {
        // Get fields from the source fields saved in the data store:
        // the number and order of the source fields must be the same as the target fields
        String groupId = hiveFullInfo.getInlongGroupId();
        String streamId = hiveFullInfo.getInlongStreamId();
        List<StreamSinkFieldEntity> fieldList = sinkFieldMapper.selectBySinkId(hiveFullInfo.getSinkId());

        if (fieldList == null || fieldList.size() == 0) {
            throw new WorkflowListenerException("fields is null for group id=" + groupId + ", stream id=" + streamId);
        }

        SourceInfo sourceInfo = getSourceInfo(groupInfo, hiveFullInfo, fieldList, topoName);
        com.tencent.oceanus.etl.protocol.sink.SinkInfo sinkInfo = getSinkInfo(hiveFullInfo, fieldList);

        // Dynamic configuration information,
        // which can be used to specify optional parameter information of source or sink
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("source.tdbank.bid", groupInfo.getInlongGroupId());

        String flowId = STORAGE_HIVE + "_" + hiveFullInfo.getId();
        if (hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE) {
            flowId = STORAGE_THIVE + "_" + hiveFullInfo.getSinkId();
        }
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
            List<StreamSinkFieldEntity> fieldList) {
        if (hiveFullInfo.getHiveAddress() == null) {
            throw new BusinessException("hive server url cannot be empty");
        }

        // Must be the field separator in hive, and the default is textfile
        Character separator = (char) Integer.parseInt(hiveFullInfo.getTargetSeparator());
        HiveFileFormatInfo fileFormat;
        String format = hiveFullInfo.getFileFormat();
        // Currently, sort does not support bizconstant.file_ FORMAT_ RC
        if (TencentConstants.FILE_FORMAT_ORC.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.OrcFileFormatInfo();
        } else if (TencentConstants.FILE_FORMAT_SEQUENCE.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.SequenceFileFormatInfo(separator, 100);
        } else if (TencentConstants.FILE_FORMAT_PARQUET.equalsIgnoreCase(format)) {
            fileFormat = new HiveSinkInfo.ParquetFileFormatInfo();
        } else {
            fileFormat = new HiveSinkInfo.TextFileFormatInfo(separator);
        }

        String createStrategy = hiveFullInfo.getPartitionCreationStrategy();
        PartitionCreationStrategy creationStrategy = PartitionCreationStrategy.COMPLETED;
        if (PART_ARRIVED.equals(createStrategy)) {
            creationStrategy = PartitionCreationStrategy.ARRIVED;
        } else if (PART_COUNT_VERIFIED.equalsIgnoreCase(createStrategy)) {
            // TODO If the data quantity passes the verification,
            //  the [absolute value error] and [relative value error] shall be pushed
            creationStrategy = PartitionCreationStrategy.AGENT_COUNT_VERIFIED;
        } /* else if (PART_DISTINCT_VERIFIED.equalsIgnoreCase(createStrategy)) {
            // The data deduplication verification is passed.
            It is only used to create us tasks. Sort is still completed for partitions
            creationStrategy = PartitionCreationStrategy.COMPLETED;
        }*/

        // dataPath = hdfsUrl + / + warehouseDir + / + dbName + .db/ + tableName
        String dataPath = hiveFullInfo.getHdfsDefaultFs() + hiveFullInfo.getWarehouseDir() + "/"
                + hiveFullInfo.getDbName() + ".db/" + hiveFullInfo.getTableName();

        // Data consistency assurance
        ConsistencyGuarantee consistency = ConsistencyGuarantee.EXACTLY_ONCE;
        String consistencyStr = hiveFullInfo.getDataConsistency();
        if (AT_LEAST_ONCE.equals(consistencyStr)) {
            consistency = ConsistencyGuarantee.AT_LEAST_ONCE;
        }

        // Get the sink field. If there is no partition field in the source field, add the partition field to the last
        List<FieldInfo> fieldInfoList = getSinkFields(fieldList, hiveFullInfo.getPrimaryPartition());

        com.tencent.oceanus.etl.protocol.sink.SinkInfo sinkInfo;
        if (hiveFullInfo.getIsThive() == TencentConstants.THIVE_TYPE) {
            if (StringUtils.isBlank(hiveFullInfo.getUsTaskId())) {
                throw new BusinessException(String.format("us task id cannot be empty for bid=%s, tid=%s",
                        hiveFullInfo.getInlongGroupId(), hiveFullInfo.getInlongStreamId()));
            }
            sinkInfo = getTHiveSinkInfo(hiveFullInfo, fileFormat, creationStrategy, dataPath, fieldInfoList,
                    consistency);
        } else {
            sinkInfo = getHiveSinkInfo(hiveFullInfo, fileFormat, creationStrategy, dataPath, fieldInfoList,
                    consistency);
        }
        return sinkInfo;
    }

    /**
     * Get hive sink information
     */
    private com.tencent.oceanus.etl.protocol.sink.SinkInfo getHiveSinkInfo(InnerHiveFullInfo hiveFullInfo,
            HiveFileFormatInfo fileFormat, PartitionCreationStrategy creationStrategy, String dataPath,
            List<FieldInfo> fieldInfoList, ConsistencyGuarantee consistency) {

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
        String hiveServerUrl = hiveFullInfo.getHiveAddress();
        if (!hiveServerUrl.startsWith("jdbc")) {
            hiveServerUrl = "jdbc:hive2://" + hiveServerUrl;
        }

        // The virtual user, namely Hadoop proxyuser, is used to write HDFS
        String user = hiveFullInfo.getVirtualUser();
        if (StringUtils.isBlank(user)) {
            user = hiveFullInfo.getCreator();
        }
        return new HiveSinkInfo(fieldInfoList.toArray(new FieldInfo[0]), hiveServerUrl, hiveFullInfo.getDbName(),
                hiveFullInfo.getTableName(), hiveFullInfo.getUsername(), hiveFullInfo.getPassword(),
                dataPath, user, creationStrategy,
                partitionList.toArray(new HiveSinkInfo.HivePartitionInfo[0]),
                fileFormat, consistency);
    }

    /**
     * Get thive sink information
     *
     * @apiNote Thive does not support secondary partition at present
     */
    private com.tencent.oceanus.etl.protocol.sink.SinkInfo getTHiveSinkInfo(InnerHiveFullInfo hiveFullInfo,
            HiveFileFormatInfo fileFormat, PartitionCreationStrategy creationStrategy, String dataPath,
            List<FieldInfo> fieldInfoList, ConsistencyGuarantee consistency) {

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
        String hiveServerUrl = hiveFullInfo.getHiveAddress();
        if (!hiveServerUrl.startsWith("jdbc")) {
            hiveServerUrl = "jdbc:hive://" + hiveServerUrl;
        }

        // info.getUsername(); // Create partitions through the thive JDBC server link
        // info.getCreator() - tdwUsername // Go to TDW to query whether the partition exists
        String hadoopProxyUser = "tdwadmin"; // Sort write HDFS use
        return new THiveSinkInfo(fieldInfoList.toArray(new FieldInfo[0]), hiveServerUrl,
                hiveFullInfo.getDbName(), hiveFullInfo.getTableName(),
                hiveFullInfo.getUsername(), hiveFullInfo.getPassword(),
                dataPath, hadoopProxyUser,
                creationStrategy, partitionInfos.toArray(new THiveSinkInfo.THivePartitionInfo[0]),
                fileFormat, hiveFullInfo.getAppGroupName(),
                hiveFullInfo.getCreator(), consistency,
                hiveFullInfo.getUsTaskId());
    }

    /**
     * Get the sink field information.
     * If there is a partition field in the common field, use it. Otherwise, add the partition field to the last
     */
    private List<FieldInfo> getSinkFields(List<StreamSinkFieldEntity> fieldList, String partitionField) {
        boolean duplicate = false;
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            String fieldName = field.getFieldName();
            if (fieldName.equals(partitionField)) {
                duplicate = true;
            }

            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getFieldType().toLowerCase());
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
            List<StreamSinkFieldEntity> fieldList, String topoName) {
        List<InlongGroupExtInfo> extInfos = groupInfo.getExtList();
        Map<String, String> extInfosMap = extInfos.stream()
                .collect(Collectors.toMap(InlongGroupExtInfo::getKeyName, InlongGroupExtInfo::getKeyValue));
        String streamId = hiveFullInfo.getInlongStreamId();

        // First determine the data source type. Tddmsgdbsync is temporarily used for DB
        DeserializationInfo deserializationInfo = null;
        boolean isDbType = "DB".equals(hiveFullInfo.getDataSourceType());
        if (isDbType) {
            deserializationInfo = new TDMsgDBSyncDeserializationInfo(streamId);
        } else {
            // File and self pushed source. The data format is text or key-value, or CSV. Tdmsgcsv is temporarily used
            deserializationInfo = getDeserializationInfo(hiveFullInfo);
        }
        // Source fields are to be obtained from the source fields saved in the data store:
        // the number and order of source fields must be the same as the target fields
        SourceInfo sourceInfo = null;
        // Get the source field. If there is no partition field in the source, add the partition field to the last
        List<FieldInfo> sourceFields = getSourceFields(fieldList, hiveFullInfo.getPrimaryPartition());

        String groupId = groupInfo.getInlongGroupId();
        String mqType = groupInfo.getMqType();
        if (MQType.TUBEMQ.equalsIgnoreCase(mqType)) {
            InlongClusterEntity cluster = inlongClusterEntityMapper.selectByKey(groupInfo.getInlongClusterTag(), null,
                    MQType.TUBEMQ).get(0);
            Preconditions.checkNotNull(cluster, "tube cluster not found for bid=" + groupId);
            Integer tubeId = cluster.getId();
            TubeClusterDTO tubeClusterDTO = TubeClusterDTO.getFromJson(cluster.getExtParams());
            String masterAddress = tubeClusterDTO.getMasterWebUrl();
            Preconditions.checkNotNull(masterAddress, "tube cluster [" + tubeId + "] not contains masterAddress");

            String topic = groupInfo.getMqResource();
            String consumerGroup = getConsumerGroup(groupId, null, topic, topoName,
                    MQType.PULSAR);
            sourceInfo = new TubeSourceInfo(topic, masterAddress, consumerGroup,
                    deserializationInfo, sourceFields.toArray(new FieldInfo[0]));
        } else if (MQType.PULSAR.equalsIgnoreCase(mqType)) {
            String tenant = "tenant_" + extInfosMap.get("appGroupName");
            String namespace = groupInfo.getMqResource();
            String topic = hiveFullInfo.getMqResourceObj();
            // Full path of topic in pulsar
            String fullTopic = "persistent://" + tenant + "/" + namespace + "/" + topic;
            try {
                InlongClusterEntity cluster = inlongClusterEntityMapper.selectByKey(
                        groupInfo.getInlongClusterTag(), null, MQType.PULSAR).get(0);
                // Multiple adminurls should be configured for pulsar,
                // otherwise all requests will be sent to the same broker
                PulsarClusterDTO pulsarClusterDTO = PulsarClusterDTO.getFromJson(cluster.getExtParams());
                String adminUrl = pulsarClusterDTO.getAdminUrl();
                String masterAddress = cluster.getUrl();
                // Ensure compatibility of old data: if the old subscription exists, use the old one;
                // otherwise, create the subscription according to the new rule
                String subscription = getConsumerGroup(groupId, streamId, topic, topoName, MQType.PULSAR);
                sourceInfo = new PulsarSourceInfo(adminUrl, masterAddress, fullTopic, subscription,
                        deserializationInfo, sourceFields.toArray(new FieldInfo[0]));
            } catch (Exception e) {
                LOGGER.error("get pulsar information failed", e);
                throw new WorkflowListenerException("get pulsar admin failed, reason: " + e.getMessage());
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
    private List<FieldInfo> getSourceFields(List<StreamSinkFieldEntity> fieldList, String partitionField) {
        boolean duplicate = false;
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(
                    field.getSourceFieldType().toLowerCase());
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

    /**
     * Get deserializationinfo object information
     */
    private DeserializationInfo getDeserializationInfo(InnerHiveFullInfo hiveFullInfo) {
        String dataType = hiveFullInfo.getDataType();
        Character escape = null;
        DeserializationInfo deserializationInfo;
        if (hiveFullInfo.getDataEscapeChar() != null) {
            escape = hiveFullInfo.getDataEscapeChar().charAt(0);
        }
        // Must be a field separator in the data stream
        char separator = (char) Integer.parseInt(hiveFullInfo.getSourceSeparator());
        if (TencentConstants.DATA_TYPE_TEXT.equalsIgnoreCase(dataType)) {
            // Do you want to delete the first separator? The default is false
            deserializationInfo = new TDMsgCsvDeserializationInfo(hiveFullInfo.getInlongStreamId(), separator, escape,
                    false);
        } else if (TencentConstants.DATA_TYPE_KEY_VALUE.equalsIgnoreCase(dataType)) {
            // KV pair separator, which must be the field separator in the data flow
            char kvSeparator = '&';
            InlongStreamInfo streamInfo = inlongStreamService.get(hiveFullInfo.getInlongGroupId(),
                    hiveFullInfo.getInlongStreamId());
            List<InlongStreamExtInfo> extInfos = streamInfo.getExtList();
            Map<String, String> extInfosMap = extInfos.stream()
                    .collect(Collectors.toMap(InlongStreamExtInfo::getKeyName, InlongStreamExtInfo::getKeyValue));
            if (extInfosMap.get("kvSeparator") != null) {
                kvSeparator = (char) Integer.parseInt(extInfosMap.get("kvSeparator"));
            }
            // Row separator, which must be a field separator in the data flow
            Character lineSeparator = null;
            if (hiveFullInfo.getLineSeparator() != null) {
                lineSeparator = (char) Integer.parseInt(hiveFullInfo.getLineSeparator());
            }
            deserializationInfo = new TDMsgKvDeserializationInfo(hiveFullInfo.getInlongStreamId(), separator,
                    kvSeparator,
                    escape, lineSeparator);
        } else if (TencentConstants.DATA_TYPE_CSV.equalsIgnoreCase(dataType)) {
            deserializationInfo = new CsvDeserializationInfo(separator, escape);
        } else {
            throw new IllegalArgumentException("can not support sink data type:" + dataType);
        }
        return deserializationInfo;
    }

    private String getZkRootForPulsar(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getPulsarRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.PULSAR_DEFAULT;
        }
        return zkRoot;
    }

    private String getZkRootForTube(ZkClusterDTO zkClusterDTO) {
        String zkRoot = zkClusterDTO.getTubeRoot();
        if (StringUtils.isBlank(zkRoot)) {
            zkRoot = TencentConstants.TUBE_DEFAULT;
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
     * Remove hive sort configuration on zookeeper
     */
    public void deleteHiveConfig(InlongGroupInfo groupInfo, Integer sinkId) throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        if (sinkId == null) {
            LOGGER.warn("no need to delete hive config for bid={}, as no hive storage exists", groupId);
            return;
        }

        List<InlongClusterEntity> zkClusters = inlongClusterEntityMapper.selectByKey(groupInfo.getInlongClusterTag(),
                null, ClusterType.ZOOKEEPER);
        if (CollectionUtils.isEmpty(zkClusters) || StringUtils.isBlank(zkClusters.get(0).getUrl())) {
            throw new WorkflowListenerException("sort zk cluster not found for groupId=" + groupId);
        }
        InlongClusterEntity zkCluster = zkClusters.get(0);
        ZkClusterDTO zkClusterDTO = ZkClusterDTO.getFromJson(zkCluster.getExtParams());
        String topoName = clusterHiveTopo;
        if (topoName == null || StringUtils.isBlank(topoName)) {
            throw new WorkflowListenerException("hive topo cluster not found for groupId=" + groupId);
        }

        String zkUrl = zkCluster.getUrl();
        String zkRoot = getZkRoot(groupInfo.getMqType(), zkClusterDTO);
        LOGGER.info("try to delete hive sort config from {}, idList={}", zkUrl, sinkId);
        // It could be hive or thive
        ZkTools.removeDataFlowFromCluster(topoName, STORAGE_HIVE + "_" + sinkId, zkUrl, zkRoot);
        ZkTools.removeDataFlowFromCluster(topoName, STORAGE_THIVE + "_" + sinkId, zkUrl, zkRoot);
        LOGGER.info("success to delete hive sort config from {}, idList={}", zkUrl, sinkId);
    }

}
