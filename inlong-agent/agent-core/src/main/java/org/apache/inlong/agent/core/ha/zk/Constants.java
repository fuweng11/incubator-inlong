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
     * /DbSync/update_switch, 升级时，关闭调度的开关，内容{"updating",true/false}
     * true : 升级中，停止协调者调度
     * false : 非升级中，正常协调调度
     */
    public static final String SYNC_JOB_UPDATE_SWITCH_PATH = "/DbSync/update_switch";

    /*
     * 每个中转机，集群下的任务根目录
     */
    public static final String SYNC_JOB_PARENT_PATH = "/DbSync/%s/job";

    public static final String SYNC_JOB_PATH_MATCH = "/DbSync/.*/job";

    public static final String SYNC_PREFIX = "/DbSync";

    /*
     * 每个中转机，集群下的协调者，通过顺序临时节点竞争获得
     */
    public static final String SYNC_JOB_COORDINATOR_NODE_PATH = "/DbSync/%s/coordinator";

    public static final String SYNC_JOB_COORDINATOR_PATH_MATCH = "/DbSync/.*/coordinator/.*";
    /*
     * 每个中转机，集群下的协调者，通过顺序临时节点竞争获得
     */
    public static final String SYNC_JOB_CANDIDATE_NODE_PATH = "/DbSync/%s/candidate";

    public static final String SYNC_JOB_CANDIDATE_PATH_MATCH = "/DbSync/.*/candidate/.*";

    /*
     * 每个任务，分配到的dbsync 机器的节点信息
     */
    public static final String SYNC_JOB_RUN_PATH = SYNC_JOB_PARENT_PATH + "/%s/run";

    public static final String SYNC_JOB_RUN_PATH_MATCH = SYNC_JOB_PATH_MATCH + "/.*/run";

    /*
     * 每个任务，执行保存的最新，位置信息
     */
    public static final String SYNC_JOB_POSITION_PATH = SYNC_JOB_PARENT_PATH + "/%s/position";

}
