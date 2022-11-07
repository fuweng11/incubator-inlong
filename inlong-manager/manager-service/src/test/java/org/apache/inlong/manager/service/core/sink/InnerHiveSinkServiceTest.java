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

package org.apache.inlong.manager.service.core.sink;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.node.tencent.hive.InnerHiveDataNodeRequest;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveSink;
import org.apache.inlong.manager.pojo.sink.tencent.hive.InnerHiveSinkRequest;
import org.apache.inlong.manager.pojo.tencent.us.USConfiguration;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.node.DataNodeService;
import org.apache.inlong.manager.service.resource.sink.SinkResourceOperatorFactory;
import org.apache.inlong.manager.service.resource.sort.SortConfigOperatorFactory;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Hive sink service test
 */
public class InnerHiveSinkServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "admin";
    private final String sinkName = "default";

    @Autowired
    private StreamSinkService sinkService;

    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;

    @Autowired
    private DataNodeService dataNodeService;

    @Autowired
    private SinkResourceOperatorFactory resourceOperatorFactory;
    @Autowired
    private SortConfigOperatorFactory operatorFactory;
    @Autowired
    private InlongGroupService inlongGroupService;
    @Autowired
    private InlongStreamService inlongStreamService;
    @Autowired
    private USConfiguration usConfiguration;

    /**
     * Save sink info.
     */
    public Integer saveSink(Integer dataNodeId) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        InnerHiveSinkRequest sinkInfo = new InnerHiveSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.INNER_HIVE);
        sinkInfo.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        sinkInfo.setSinkName(sinkName);
        sinkInfo.setAppGroupName("g_test_thirty");
        sinkInfo.setDataConsistency("EXACTLY_ONCE");
        sinkInfo.setDataEncoding("UTF-8");
        sinkInfo.setDataSeparator("9");
        sinkInfo.setDbName("tdmthivel");
        sinkInfo.setFileFormat("TextFile");
        sinkInfo.setPartitionCreationStrategy("ARRIVED");
        sinkInfo.setPartitionInterval(1);
        sinkInfo.setPartitionType("LIST");
        sinkInfo.setTableName("person");
        SinkField sinkField = new SinkField();
        sinkField.setFieldComment("");
        sinkField.setFieldFormat("0");
        sinkField.setFieldName("name");
        sinkField.setFieldType("string");
        sinkField.setIsMetaField(0);
        sinkField.setSourceFieldName("name");
        sinkField.setSourceFieldType("string");
        List<SinkField> sinkFieldList = new ArrayList<>();
        sinkFieldList.add(sinkField);
        sinkInfo.setSinkFieldList(sinkFieldList);
        sinkInfo.setEnableCreateResource(1);
        return sinkService.save(sinkInfo, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer dataNodeId = this.saveDataNode();
        Integer id = this.saveSink(dataNodeId);
        Assertions.assertNotNull(id);

        boolean result = sinkService.delete(id, false, globalOperator);
        Assertions.assertTrue(result);
        boolean deleteDataNodeResult = dataNodeService.delete(dataNodeId, globalOperator);
        Assertions.assertTrue(deleteDataNodeResult);
    }

    private Integer saveDataNode() {
        String nodeName = "hiveNode2";
        String type = "INNER_HIVE";
        String url = "127.0.0.1:8080";
        String username = "admin";
        String password = "123";
        InnerHiveDataNodeRequest request = new InnerHiveDataNodeRequest();
        request.setName(nodeName);
        request.setType(type);
        request.setUrl(url);
        request.setUsername(username);
        request.setToken(password);
        request.setInCharges(GLOBAL_OPERATOR);
        request.setHiveAddress("127.0.0.1");
        request.setToken("123456");
        request.setWarehouseDir("/test");
        request.setHdfsDefaultFs("hdfs://test/");
        request.setHdfsUgi("tdwadmin:test");
        request.setClusterTag("tl");
        return dataNodeService.save(request, GLOBAL_OPERATOR);
    }

    @Test
    public void testListByIdentifier() {
        Integer dataNodeId = this.saveDataNode();
        Integer id = this.saveSink(dataNodeId);
        StreamSink sink = sinkService.get(id);
        Assertions.assertEquals(globalGroupId, sink.getInlongGroupId());

        sinkService.delete(id, false, globalOperator);
        boolean deleteDataNodeResult = dataNodeService.delete(dataNodeId, globalOperator);
        Assertions.assertTrue(deleteDataNodeResult);
    }

    @Test
    public void testGetAndUpdate() {
        Integer dataNodeId = this.saveDataNode();
        Integer sinkId = this.saveSink(dataNodeId);
        StreamSink streamSink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, streamSink.getInlongGroupId());

        InnerHiveSink sink = (InnerHiveSink) streamSink;
        sink.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        InnerHiveSinkRequest request = CommonBeanUtils.copyProperties(sink, InnerHiveSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assertions.assertTrue(result);

        sinkService.delete(sinkId, false, globalOperator);
        boolean deleteDataNodeResult = dataNodeService.delete(dataNodeId, globalOperator);
        Assertions.assertTrue(deleteDataNodeResult);
    }
}
