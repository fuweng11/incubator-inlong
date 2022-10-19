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

import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.inlong.agent.core.ha.JobHaDispatcherImpl;
import org.apache.inlong.agent.core.ha.zk.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
