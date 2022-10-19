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

package org.apache.inlong.agent.mysql.relaylog;

import org.apache.commons.io.FileUtils;
import org.apache.inlong.agent.conf.AgentConfiguration;

import java.io.File;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_BLOG_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_FILE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_RELAY_LOG_BLOG_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_RELAY_LOG_FILE_SIZE;

public abstract class AbstractRelayLog implements RelayLog {

    protected final String relayPrefix;
    protected final String logPath;
    protected final long fileSize;
    protected final int blockSize;

    protected AgentConfiguration config;

    public AbstractRelayLog(String relayPrefix, String logPath) {
        if (relayPrefix == null || logPath == null) {
            throw new IllegalArgumentException("log prefix & log root path can't be null");
        }

        this.config = AgentConfiguration.getAgentConf();
        this.logPath = logPath.endsWith("/") ? logPath : logPath + "/";
        checkDir(this.logPath);
        this.relayPrefix = relayPrefix;

        fileSize = config.getInt(DBSYNC_RELAY_LOG_FILE_SIZE, DEFAULT_DBSYNC_RELAY_LOG_FILE_SIZE);
        blockSize = config.getInt(DBSYNC_RELAY_LOG_BLOG_SIZE, DEFAULT_DBSYNC_RELAY_LOG_BLOG_SIZE);
    }

    protected void checkDir(final String path) {
        File dir = new File(path);
        FileUtils.deleteQuietly(dir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Create directory failed: " + dir.getAbsolutePath());
            }
        }
    }
}
