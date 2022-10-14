package org.apache.inlong.agent.core.ha.listener;

import com.google.gson.Gson;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class UpdateSwitchNodeChangeListener
        implements TreeCacheListener {

    private Logger logger = LogManager.getLogger(UpdateSwitchNodeChangeListener.class);

    private JobHaDispatcherImpl dispatcher;

    private Gson gson = new Gson();

    public UpdateSwitchNodeChangeListener(JobHaDispatcherImpl dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event)
            throws Exception {
        if (event.getData() == null) {
            return;
        }
        String eventPath = event.getData().getPath();
        logger.info("path: {}, eventType: {}", event.getData().getPath(), event.getType());
        if (!eventPath.equals(Constants.SYNC_JOB_UPDATE_SWITCH_PATH)) {
            return;
        }

        switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
                dispatcher.updateIsUpdating();
                break;
            case CONNECTION_LOST:
                logger.info("connection lost===============");
                break;
            case CONNECTION_RECONNECTED:
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
