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

package org.apache.inlong.agent.core;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.core.dbsync.DbAgentHeartbeatManager;
import org.apache.inlong.agent.core.dbsync.DbAgentJobManager;
import org.apache.inlong.agent.core.dbsync.DbColumnChangeManager;
import org.apache.inlong.agent.core.task.ITaskPositionManager;
import org.apache.inlong.agent.core.task.PositionManager;
import org.apache.inlong.agent.core.task.TaskManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;

public class DbAgentManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentManager.class);

    private static final String DEFAULT_FETCHER = "org.apache.inlong.agent.plugin.fetcher.DbAgentBinlogFetcher";
    private final DbAgentJobManager dbSyncJobManager;
    private final TaskManager taskManager;
    private final DbAgentHeartbeatManager heartbeatManager;
    private final ProfileFetcher fetcher;
    private final AgentConfiguration conf;
    private final ITaskPositionManager taskPositionManager;
    private DbColumnChangeManager fieldManager;
    private boolean enableReportFiledChange;
    private volatile boolean agentIsRunning = true;
    private final MetricReportThread metricReportThread;
    private final HotConfReplaceThread hotConfReplaceThread;

    public DbAgentManager() {
        conf = AgentConfiguration.getAgentConf();
        dbSyncJobManager = new DbAgentJobManager(this);
        /*
         * 单例初始化，避免npe
         */
        taskPositionManager = PositionManager.getInstance(this);
        heartbeatManager = DbAgentHeartbeatManager.getInstance(this);
        taskManager = new TaskManager();
        fetcher = initFetcher(this);
        this.enableReportFiledChange = conf.getBoolean(AgentConstants.DBSYNC_FILED_CHANGED_REPORT_ENABLE,
                AgentConstants.DEFAULT_DBSYNC_FILED_CHANGED_REPORT_ENABLE);
        if (enableReportFiledChange) {
            this.fieldManager = DbColumnChangeManager.getInstance();
        }
        metricReportThread = new MetricReportThread();
        hotConfReplaceThread = new HotConfReplaceThread();
    }

    /**
     * init fetch by class name
     */
    private ProfileFetcher initFetcher(DbAgentManager agentManager) {
        try {
            Constructor<?> constructor =
                    Class.forName(conf.get(AgentConstants.AGENT_FETCHER_CLASSNAME, DEFAULT_FETCHER))
                            .getDeclaredConstructor(DbAgentManager.class);
            constructor.setAccessible(true);
            return (ProfileFetcher) constructor.newInstance(agentManager);
        } catch (Exception ex) {
            LOGGER.warn("cannot find fetcher: ", ex);
        }
        return null;
    }

    public DbAgentJobManager getJobManager() {
        return dbSyncJobManager;
    }

    public ProfileFetcher getFetcher() {
        return fetcher;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    @Override
    public void join() {
        super.join();
        dbSyncJobManager.join();
        taskManager.join();
    }

    /*
     * start order : 启动动态加载配置变更线程 ---> 启动binlog job 管理 ---> 启动agent task manager ---> 启动心跳 ---> 启动字段变更上报管理 ---> 启动动态从tdm
     * 拉去取配置
     */
    @Override
    public void start() throws Exception {
        LOGGER.info("starting agent manager");
        /*
         * 动态更新配置文件
         */
        submitWorker(hotConfReplaceThread);
        /*
         * 周期输出系统指标
         */
        submitWorker(metricReportThread);

        /*
         * 启动dbsync job manager
         */
        dbSyncJobManager.start();

        /*
         * 启动拉取 任务管理
         */
        taskManager.start();

        /*
         * 启动心跳，dbsync 部分有两个心跳，一个是agent 心跳，一个是采集状态心跳
         */
        heartbeatManager.start();

        /*
         * 上报 db 表字段变更服务
         */
        if (fieldManager != null) {
            fieldManager.start();
        }

        /*
         * 负责具体的拉取配置，只有拉取到具体的配置并且加载完成后，才会真正的开始进行 binlog 采集
         */
        fetcher.start();

    }

    /**
     * It should guarantee thread-safe, and can be invoked many times.
     *
     * @throws Exception exceptions
     */
    @Override
    public void stop() throws Exception {
        LOGGER.info("begin stop db agent ----- ");
        agentIsRunning = false;
        /*
         * 停止获取配置变更
         */
        fetcher.stop();

        /*
         * 停止job manager
         */
        dbSyncJobManager.stop();

        /*
         * 停止task manager
         */
        taskManager.stop();

        /*
         * 停止字段变更上报
         */
        if (fieldManager != null) {
            fieldManager.stop();
        }

        /*
         * 停止心跳
         */
        heartbeatManager.stop();

        LOGGER.info("End stop db agent ----- ");
    }

    class HotConfReplaceThread implements Runnable {

        private long lastModifiedTime = 0L;
        @Override
        public void run() {
            while (isRunning()) {
                try {
                    Thread.sleep(60 * 1000); // 10s check
                    File file = new File(
                            conf.getConfigLocation(AgentConfiguration.DEFAULT_CONFIG_FILE).getFile());
                    if (!file.exists()) {
                        continue;
                    }
                    if (file.lastModified() > lastModifiedTime) {
                        conf.reloadFromLocalPropertiesFile();
                        lastModifiedTime = file.lastModified();
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted when flush agent conf.", e);
                }
            }
        }
    }

    class MetricReportThread implements Runnable {

        @Override
        public void run() {
            while (isRunning()) {
                try {
                    Thread.sleep(60 * 1000); // 10s check
                    if (dbSyncJobManager != null) {
                        dbSyncJobManager.report();
                    }
                } catch (Throwable e) {
                    LOGGER.error("MetricReportThread has exception !", e);
                }
            }
        }
    }

    private Boolean isRunning() {
        return agentIsRunning;
    }
}
