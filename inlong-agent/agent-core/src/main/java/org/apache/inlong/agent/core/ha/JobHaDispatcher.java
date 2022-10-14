package org.apache.inlong.agent.core.ha;

import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncTaskInfo;

import java.util.List;
import java.util.Set;

public interface JobHaDispatcher {

    /**
     * add job
     */
    void addJob(DbSyncTaskInfo taskConf);

    /**
     * update job
     */
    void updateJob(DbSyncTaskInfo taskConf);

    /**
     * start job but it may be not running
     */
    void startJob(DbSyncTaskInfo taskConf);

    /**
     * start job but it may be not running
     */
    List<DBSyncJob> startAllTasksOnce(List<DbSyncTaskInfo> taskConf);

    /**
     * stop job
     */
    void stopJob(String syncId, Integer taskId);


    /**
     * delete job
     *
     * @param syncId
     * @param taskId
     * @param taskConfFromTdm
     */
    void deleteJob(String syncId, Integer taskId, DbSyncTaskInfo taskConfFromTdm);

    /**
     * stop running job
     */
    void removeLocalRunJob(String syncId);

    /**
     * start running when it is started
     */
    void updateRunJobInfo(String syncId, JobRunNodeInfo jobRunNodeInfo);

    /**
     * @param clusterId
     */
    void updateJobCoordinator(String clusterId, String changeCoordinatorPath);

    /**
     *
     */
    void updateZkStats(String clusterId, String syncId, boolean isConnected);

    /**
     * @return
     */
    Set<JobHaInfo> getHaJobInfList();

    /**
     * @return
     */
    List<DbSyncTaskInfo> getErrorTaskConfInfList();

    /**
     * @return
     */
    List<DbSyncTaskInfo> getCorrectTaskConfInfList();

    /**
     * @return
     */
    List<DbSyncTaskInfo> getExceedTaskInfList();

    /**
     * @return
     */
    boolean updateSyncIdList(Integer clusterId, Integer syncIdListVersion, String zkServer, List<String> syncIdList,
            boolean needCheckClusterIdOrServerIdListVersion);

    /**
     * @param syncId syncId
     * @param position postition
     */
    void updatePosition(String syncId, String position);

    Integer getClusterId();

    Integer getSyncIdListVersion();

    Set<String> getNeedToRunSyncIdSet();

    void updateIsUpdating();

    boolean isDbsyncUpdating();

    boolean isCoordinator();

    String getZkUrl();

    ConfigDelegate getZkConfigDelegate();

    int getMaxSyncIdsThreshold();
}
