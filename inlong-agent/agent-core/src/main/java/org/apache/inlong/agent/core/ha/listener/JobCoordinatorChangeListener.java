package org.apache.inlong.agent.core.ha.listener;

import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class JobCoordinatorChangeListener
        implements PathChildrenCacheListener {

    private Logger logger = LogManager.getLogger(JobCoordinatorChangeListener.class);

    private JobHaDispatcherImpl dispatcher;

    public JobCoordinatorChangeListener(JobHaDispatcherImpl dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws Exception {
        if (event.getData() == null) {
            return;
         }
        String eventPath = event.getData().getPath();
        String clusterId = ConfigDelegate.getClusterIdFromZKPath(eventPath);
        logger.info("path: {}, eventType: {}, clusterId: {}", event.getData().getPath(),
                event.getType(), clusterId);
        if (!eventPath.matches(Constants.SYNC_JOB_COORDINATOR_PATH_MATCH)) {
            return;
        }
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_REMOVED:
                try {
                    dispatcher.updateJobCoordinator(clusterId, ConfigDelegate.getLastFromZKPath(eventPath));
                } catch (Exception e) {
                    logger.error("updateJobLeaderShip has exception e = {}", e);
                }
                break;
            case CHILD_UPDATED:
                logger.info("update lost===============");
                break;
            case CONNECTION_LOST:
                logger.info("connection lost===============");
                dispatcher.updateZkStats(clusterId, null,false);
                break;
            case CONNECTION_RECONNECTED:
                dispatcher.updateZkStats(clusterId, null,true);
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
