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

package org.apache.inlong.manager.service.resource.sc;

import org.apache.inlong.manager.pojo.tencent.sc.AppGroup;
import org.apache.inlong.manager.pojo.tencent.sc.Product;
import org.apache.inlong.manager.pojo.tencent.sc.ScHiveResource;
import org.apache.inlong.manager.pojo.tencent.sc.Staff;

import java.util.List;

/**
 * Security Center Service
 */
public interface ScService {

    /**
     * Fuzzy search by employee name
     *
     * @param name Employee name
     * @return Employee list
     */
    List<Staff> listStaff(String name);

    /**
     * Get the cluster ID according to the cluster identifier
     *
     * @param identifier Cluster identifier
     * @return Cluster ID
     */
    Integer getClusterIdByIdentifier(String identifier);

    /**
     * Get product details according to product ID
     *
     * @param id Produce ID
     * @return Product details
     */
    Product getProduct(Integer id);

    /**
     * Fuzzy query the product list of the specified user
     *
     * @param username Username
     * @param productName Product name
     * @return Produce list
     */
    List<Product> listProduct(String username, String productName);

    /**
     * Query application group details according to application group ID
     *
     * @param id App group ID
     * @return App group detail
     */
    AppGroup getAppGroup(Integer id);

    /**
     * Query application group details according to application group name
     *
     * @param clusterId Cluster ID
     * @param groupName App group
     * @return App group detail
     */
    AppGroup getAppGroup(Integer clusterId, String groupName);

    /**
     * Get all application groups of the current user under the current product
     *
     * @param productId Product ID
     * @param username Username
     * @return Product list
     */
    List<String> listAppGroupByUser(Integer productId, String username);

    /**
     * Fuzzy query application group list according to application group name
     *
     * @param name App group name
     * @return App group list
     */
    List<AppGroup> listAllAppGroup(String name);

    /**
     * Fuzzy query application group list according to application group name
     *
     * @param groupId inlong group id
     * @param dataNodeName data node name
     * @param sinkType sink type
     * @return Database list
     */
    List<ScHiveResource> listDatabase(String groupId, String dataNodeName, String sinkType);

    /**
     * Check whether there is corresponding permission according to the username
     *
     * @param username username
     * @param database database
     * @param table table
     * @param accessType access type
     * @return true or false
     */
    boolean checkPermissions(String username, String database, String table, String accessType);

    /**
     * Grant permission according to the username
     *
     * @param username username
     * @param database database
     * @param table table
     * @param accessType access type
     * @return true or false
     */
    boolean grant(String username, String database, String table, String accessType, String hiveType);

}
