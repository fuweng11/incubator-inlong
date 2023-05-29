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

package org.apache.inlong.agent.conf;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.constant.AgentConstants;

import org.apache.commons.io.FileUtils;
import org.apache.inlong.agent.mysql.connector.MysqlConnection.BinlogFormat;
import org.apache.inlong.agent.mysql.connector.MysqlConnection.BinlogImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MAX_CON_DB_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MYSQL_BINLOG_FORMATS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MYSQL_BINLOG_IMAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_WAY;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_ENABLE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_MAX_CON_DB_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MYSQL_BINLOG_FORMATS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MYSQL_BINLOG_IMAGES;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_RELAY_LOG_WAY;

/**
 * agent configuration. Only one instance in the process.
 * Basically it use properties file to store configurations.
 */
public class AgentConfiguration extends AbstractConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentConfiguration.class);

    public static final String DEFAULT_CONFIG_FILE = "agent.properties";
    public static final String TMP_CONFIG_FILE = ".tmp.agent.properties";

    private static final ArrayList<String> LOCAL_RESOURCES = new ArrayList<>();

    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static volatile AgentConfiguration agentConf = null;

    static {
        LOCAL_RESOURCES.add(DEFAULT_CONFIG_FILE);
    }

    /**
     * load config from agent file.
     */
    private AgentConfiguration() {
        for (String fileName : LOCAL_RESOURCES) {
            super.loadPropertiesResource(fileName);
        }
    }

    /**
     * singleton for agent configuration.
     *
     * @return static instance of AgentConfiguration
     */
    public static AgentConfiguration getAgentConf() {
        if (agentConf == null) {
            synchronized (AgentConfiguration.class) {
                if (agentConf == null) {
                    agentConf = new AgentConfiguration();
                }
            }
        }
        return agentConf;
    }

    private String getNextBackupFileName() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateStr = format.format(new Date(System.currentTimeMillis()));
        return DEFAULT_CONFIG_FILE + "." + dateStr;
    }

    /**
     * flush config to local files.
     */
    public void flushToLocalPropertiesFile() {
        LOCK.writeLock().lock();
        // TODO: flush to local file as properties file.
        try {
            String agentConfParent = get(
                    AgentConstants.AGENT_CONF_PARENT, AgentConstants.DEFAULT_AGENT_CONF_PARENT);
            File sourceFile = new File(agentConfParent, DEFAULT_CONFIG_FILE);
            File targetFile = new File(agentConfParent, getNextBackupFileName());
            File tmpFile = new File(agentConfParent, TMP_CONFIG_FILE);
            if (sourceFile.exists()) {
                FileUtils.copyFile(sourceFile, targetFile);
            }
            List<String> tmpCache = getStorageList();
            FileUtils.writeLines(tmpFile, tmpCache);

            FileUtils.copyFile(tmpFile, sourceFile);
            boolean result = tmpFile.delete();
            if (!result) {
                LOGGER.warn("cannot delete file {}", tmpFile);
            }
        } catch (Exception ex) {
            LOGGER.error("error while flush agent conf to local", ex);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    /**
     * refresh config from local files.
     * Note: there is a concurrency issue when hot-updating and fetching configuration
     */
    public void reloadFromLocalPropertiesFile() {
        for (String fileName : LOCAL_RESOURCES) {
            super.loadPropertiesResource(fileName);
        }
    }

    @Override
    public boolean allRequiredKeyExist() {
        return true;
    }

    public boolean enableHA() {
        return getBoolean(DBSYNC_ENABLE, DEFAULT_DBSYNC_ENABLE);
    }

    public BinlogFormat[] getDbSyncSupportBinlogFormats() {
        String mysqlBinlogFormat = agentConf.get(DBSYNC_MYSQL_BINLOG_FORMATS, DEFAULT_MYSQL_BINLOG_FORMATS);
        String[] formats = StringUtils.split(mysqlBinlogFormat, ',');
        if (formats == null) {
            return null;
        }
        BinlogFormat[] supportBinlogFormats = new BinlogFormat[formats.length];
        int i = 0;
        for (String format : formats) {
            supportBinlogFormats[i++] = BinlogFormat.valuesOf(format);
        }
        return supportBinlogFormats;
    }

    public BinlogImage[] getDbSyncBinlogImages() {
        String mysqlBinlogImage = agentConf.get(DBSYNC_MYSQL_BINLOG_IMAGES, DEFAULT_MYSQL_BINLOG_IMAGES);
        String[] images = StringUtils.split(mysqlBinlogImage, ',');
        if (images == null) {
            return null;
        }
        BinlogImage[] supportBinlogImages = new BinlogImage[images.length];
        int i = 0;
        for (String image : images) {
            supportBinlogImages[i++] = BinlogImage.valuesOf(image);
        }
        return supportBinlogImages;
    }

    public int getDbSyncMaxConDbSize() {
        int result = Integer.MAX_VALUE;
        String relayLogWay = agentConf.get(DBSYNC_RELAY_LOG_WAY, DEFAULT_RELAY_LOG_WAY);
        if (relayLogWay != null && relayLogWay.trim().endsWith("memory")) {
            result = agentConf.getInt(DBSYNC_MAX_CON_DB_SIZE, DEFAULT_DBSYNC_MAX_CON_DB_SIZE);
        }
        return result;
    }

}
