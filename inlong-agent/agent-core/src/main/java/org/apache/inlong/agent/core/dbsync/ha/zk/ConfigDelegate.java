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

package org.apache.inlong.agent.core.dbsync.ha.zk;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.List;

public interface ConfigDelegate extends AutoCloseable {

    String ZK_GROUP = "ZK_GROUP";

    /**
     * get cluster ID from zk path
     *
     * @param zkPath zkPath
     * @return dbJobId
     */
    static String getClusterIdFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_PREFIX)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            if (p != null && p.length >= 3) {
                return p[2];
            }
        }
        return null;
    }

    /**
     * get dbJobId from zk path
     *
     * @param zkPath zkPath
     * @return dbJobId
     */
    static String getDbJobIdFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_PREFIX)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            if (p != null && p.length >= 5) {
                return p[4];
            }
        }
        return null;
    }

    /**
     * get last node name from zk path
     *
     * @param zkPath zkPath
     * @return last node name
     */
    static String getLastFromZKPath(String zkPath) {
        if (zkPath != null && zkPath.startsWith(Constants.SYNC_JOB_PARENT_PATH)) {
            String[] p = zkPath.split(Constants.ZK_SEPARATOR);
            return p[p.length - 1];
        }
        return null;
    }

    String get(String group, String path, String key);

    byte[] getData(String group, String path);

    boolean checkPathIsExist(String group, String path);

    void createPathAndSetData(String group, String path, String data);

    String createOrderEphemeralPathAndSetData(String group, String path, String data);

    boolean deletePath(String group, String path);

    boolean createEphemeralPathAndSetData(String group, String path, String data);

    boolean createEphemeralPathAndSetDataForClient(String path, String data);

    void setOrderEphemeralPathData(String group, String path, String data);

    Integer getChildrenNum(String group, String path);

    List<String> getChildren(String group, String path);

    boolean createIfNeededPath(String group, String path);

    boolean addNodeListener(TreeCacheListener listener, String group, String path);

    boolean removeNodeListener(String group, String path);

    boolean addChildNodeListener(PathChildrenCacheListener listener, String group, String path);

    boolean removeChildNodeListener(String group, String path);
}
