package org.apache.inlong.agent.core.ha.listener;

import com.alibaba.fastjson.JSON;
import org.apache.inlong.agent.core.ha.JobRunNodeInfo;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * description：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/7/28 18:52
 * @Description:
 */
public class JobRunNodeChangeListener
        implements TreeCacheListener {

    private Logger logger = LogManager.getLogger(JobRunNodeChangeListener.class);

    private JobHaDispatcherImpl dispatcher;

    public JobRunNodeChangeListener(JobHaDispatcherImpl dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event)
            throws Exception {
        if (event.getData() == null) {
            return;
        }
        String eventPath = event.getData().getPath();
        String clusterId = ConfigDelegate.getClusterIdFromZKPath(eventPath);
        String syncId = ConfigDelegate.getSyncIdFromZKPath(eventPath);
        logger.info("path: {}, eventType: {}, clusterId: {}", event.getData().getPath(),
                event.getType(), clusterId);
        if (!eventPath.matches(Constants.SYNC_JOB_RUN_PATH_MATCH)) {
            return;
        }

        switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
                try {
                    JobRunNodeInfo jobRunNodeInfo =
                            JSON.parseObject(event.getData().getData(), JobRunNodeInfo.class);
                    dispatcher.updateRunJobInfo(syncId, jobRunNodeInfo);
                } catch (Exception e) {
                    logger.error("checkOrRunJob has exception e = {}", e);
                }
                break;
            case NODE_REMOVED:
                try {
                    dispatcher.removeLocalRunJob(syncId);
                } catch (Exception e) {
                    logger.error("checkOrStopJob has exception e = {}", e);
                }
                break;
            case CONNECTION_LOST:
                dispatcher.updateZkStats(clusterId, syncId,false);
                logger.info("connection lost===============");
                break;
            case CONNECTION_RECONNECTED:
                dispatcher.updateZkStats(clusterId, syncId, true);
                logger.info("connection reconnected ===============");
                break;
            case CONNECTION_SUSPENDED:
                logger.info("connection suspended ===============");
                break;
            default:
                break;
        }
    }



}
