package org.apache.inlong.agent.core.ha.zk;

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
     * @param syncId syncId
     * @return
     */
    public static String getJobNodeParentPath(String clusterId, String syncId) {
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
     * @param ip ip
     * @return
     */
    public static String getCandidatePath(String clusterId, String ip) {
        return String.format(Constants.SYNC_JOB_CANDIDATE_NODE_PATH,
                clusterId) + Constants.ZK_SEPARATOR + ip;
    }

    /**
     * get job position
     * @param clusterId clusterId
     * @param syncId syncId
     * @return
     */
    public static String getJobPositionPath(String clusterId, String syncId) {
        return String.format(Constants.SYNC_JOB_POSITION_PATH, clusterId, syncId);
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
     * @param syncId syncId
     * @return path
     */
    public static String getJobRunNodePath(String clusterId, String syncId) {
        return String.format(Constants.SYNC_JOB_RUN_PATH, clusterId, syncId);
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

