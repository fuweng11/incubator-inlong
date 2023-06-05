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

import org.apache.commons.lang3.StringUtils;

public class ZkUtil {

    /**
     * get candidate node patch
     * @param parentPath parent path
     * @param child ip
     * @return path
     */
    public static String getZkPath(String parentPath, String child) {
        return parentPath + Constants.ZK_SEPARATOR + child;
    }

    /**
     * get job node path
     * @param clusterId clusterId
     * @return
     */
    public static String getJobNodeParentPath(String clusterId) {
        return String.format(Constants.SYNC_JOB_PARENT_PATH, clusterId);
    }

    /**
     * get candidate parent node patch
     * @param clusterId clusterId
     * @return path
     */
    public static String getCandidateParentPath(String clusterId) {
        return String.format(Constants.SYNC_JOB_CANDIDATE_NODE_PATH, clusterId);
    }

    /**
     * get candidate node patch
     * @param clusterId clusterId
     * @param registerKey registerKey
     * @return
     */
    public static String getCandidatePath(String clusterId, String registerKey) {
        return String.format(Constants.SYNC_JOB_CANDIDATE_NODE_PATH,
                clusterId) + Constants.ZK_SEPARATOR + registerKey;
    }

    /**
     * get job position
     * @param clusterId clusterId
     * @param dbJobId dbJobId
     * @return
     */
    public static String getJobPositionPath(String clusterId, String dbJobId) {
        return String.format(Constants.SYNC_JOB_POSITION_PATH, clusterId, dbJobId);
    }

    /**
     * get candidate parent node patch
     * @param clusterId clusterId
     * @return path
     */
    public static String getCoordinatorParentPath(String clusterId) {
        return String.format(Constants.SYNC_JOB_COORDINATOR_NODE_PATH, clusterId);
    }

    /**
     * get jon run node patch
     * @param clusterId clusterId
     * @param dbJobId dbJobId
     * @return path
     */
    public static String getJobRunNodePath(String clusterId, String dbJobId) {
        return String.format(Constants.SYNC_JOB_RUN_PATH, clusterId, dbJobId);
    }

    /**
     * get jon run node patch
     * @param zkPath zkPath
     * @return path
     */
    public static String getLastNodePath(String zkPath) {
        if (StringUtils.isNotEmpty(zkPath)) {
            String[] zks = zkPath.split("/");
            return zks[zks.length - 1];
        }
        return null;
    }
}
