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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodePageRequest;
import org.apache.inlong.manager.pojo.node.hive.HiveDataNodeRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.DataNodeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Data node service test for {@link DataNodeService}
 */
public class DataNodeServiceTest extends ServiceBaseTest {

    @Autowired
    private DataNodeService dataNodeService;

    /**
     * Save data node info.
     */
    public Integer saveOpt(String nodeName, String type, String url, String username, String password) {
        HiveDataNodeRequest request = new HiveDataNodeRequest();
        request.setName(nodeName);
        request.setType(type);
        request.setUrl(url);
        request.setUsername(username);
        request.setToken(password);
        request.setDescription("test cluster");
        request.setInCharges(GLOBAL_OPERATOR);
        request.setJdbcUrl("127.0.0.1");
        request.setPassword("123456");
        return dataNodeService.save(request, GLOBAL_OPERATOR);
    }

    /**
     * Get data node list info.
     */
    public PageInfo<DataNodeInfo> listOpt(String type, String name) {
        DataNodePageRequest request = new DataNodePageRequest();
        request.setType(type);
        request.setName(name);
        return dataNodeService.list(request);
    }

    /**
     * update data node info.
     */
    public Boolean updateOpt(Integer id, String nodeName, String type, String url, String username, String password,
            Integer version) {
        HiveDataNodeRequest request = new HiveDataNodeRequest();
        request.setId(id);
        request.setName(nodeName);
        request.setType(type);
        request.setUrl(url);
        request.setUsername(username);
        request.setToken(password);
        request.setVersion(version);
        return dataNodeService.update(request, GLOBAL_OPERATOR);
    }

    /**
     * Delete data node info.
     */
    public Boolean deleteOpt(Integer id) {
        return dataNodeService.delete(id, GLOBAL_OPERATOR);
    }

    @Test
    public void testDataService() {
        String nodeName = "hiveNode1";
        String type = "HIVE";
        String url = "127.0.0.1:8080";
        String usename = "admin";
        String password = "123";

        // test save data node
        Integer id = this.saveOpt(nodeName, type, url, usename, password);
        Assertions.assertNotNull(id);

        // test get data node
        DataNodeInfo dataNodeInfo = dataNodeService.get(id);
        Assertions.assertNotNull(dataNodeInfo);
        Assertions.assertEquals(type, dataNodeInfo.getType());

        // test get data node list
        PageInfo<DataNodeInfo> listDataNode = this.listOpt(type, nodeName);
        Assertions.assertEquals(listDataNode.getTotal(), 1);

        // test update data node
        String newNodeName = "kafkaNode1";
        String newType = "HIVE";
        String newUrl = "127.0.0.1:8083";
        String newUsername = "admin2";
        String newPassword = "456";
        Integer version = listDataNode.getList().get(0).getVersion();
        System.out.println(version);
        Boolean updateSuccess = this.updateOpt(id, newNodeName, newType, newUrl, newUsername, newPassword, version);
        Assertions.assertTrue(updateSuccess);

        // test delete data node
        Boolean deleteSuccess = this.deleteOpt(id);
        Assertions.assertTrue(deleteSuccess);
    }

}
