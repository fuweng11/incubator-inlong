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

package org.apache.inlong.agent.core.ha.zk;

public class Constants {

    public static final String ZK_SEPARATOR = "/";

    public static final String ZK_FOR_LEADER = "for_coordinator";

    /*
     *  DbSync
     *     |----->update_switch
     *     |----->clusterId1
     *     |          |
     *     |          |----->coordinator
     *     |          |     |-----> for_leader_00001
     *     |          |     |-----> for_leader_00002
     *     |          |
     *     |          |----->candidate
     *     |          |    |-----> IP1
     *     |          |    |-----> IP2
     *     |          |
     *     |          |----->job
     *     |                |-----> serverId1
     *     |                |     |-----> run
     *     |                |     |-----> position
     *     |                |
     *     |                |-----> serverId2
     *     |
     *     |----->clusterId2
     */
    /*
     * /DbSync/update_switch, value:{"updating",true/false}
     * true: is updating, stop coordinating
     * false: is not updating
     */
    public static final String SYNC_JOB_UPDATE_SWITCH_PATH = "/DbSync/update_switch";

    /*
     * the root of cluster job
     */
    public static final String SYNC_JOB_PARENT_PATH = "/DbSync/%s/job";

    public static final String SYNC_JOB_PATH_MATCH = "/DbSync/.*/job";

    public static final String SYNC_PREFIX = "/DbSync";

    public static final String SYNC_JOB_COORDINATOR_NODE_PATH = "/DbSync/%s/coordinator";

    public static final String SYNC_JOB_COORDINATOR_PATH_MATCH = "/DbSync/.*/coordinator/.*";
    public static final String SYNC_JOB_CANDIDATE_NODE_PATH = "/DbSync/%s/candidate";

    public static final String SYNC_JOB_CANDIDATE_PATH_MATCH = "/DbSync/.*/candidate/.*";

    /*
     * info of dbsync node
     */
    public static final String SYNC_JOB_RUN_PATH = SYNC_JOB_PARENT_PATH + "/%s/run";

    public static final String SYNC_JOB_RUN_PATH_MATCH = SYNC_JOB_PATH_MATCH + "/.*/run";

    /*
     * position info
     */
    public static final String SYNC_JOB_POSITION_PATH = SYNC_JOB_PARENT_PATH + "/%s/position";

}
