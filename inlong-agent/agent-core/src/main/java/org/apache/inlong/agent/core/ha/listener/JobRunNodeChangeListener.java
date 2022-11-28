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

package org.apache.inlong.agent.core.ha.listener;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.ha.JobRunNodeInfo;
import org.apache.inlong.agent.core.ha.zk.ConfigDelegate;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * description: job ha
 */
public class JobRunNodeChangeListener
        implements
            TreeCacheListener {

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
                dispatcher.updateZkStats(clusterId, syncId, false);
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
