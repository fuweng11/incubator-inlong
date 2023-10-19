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

package org.apache.inlong.manager.plugin.util;

import org.apache.inlong.manager.plugin.oceanus.dto.OceanusConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Configuration file for Flink, only one instance in the process.
 * Basically it used properties file to store.
 */
public class OceanusConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(OceanusConfiguration.class);

    private static final String DEFAULT_CONFIG_FILE = "flink-sort-plugin.properties";
    private static final String INLONG_MANAGER = "inlong-manager";
    private static final String OCEANUS_OPEN_API = "oceanus.open.api";
    private static final String AUTH_USER = "oceanus.auth.user";
    private static final String AUTH_CMK = "oceanus.auth.cmk";
    private static final String SORT_DIS_FILE_NAME = "sort.dist.filename";
    private static final String SOURCE_CONNECT_FILE_NAME = "source.connect.filename";
    private static final String SINK_CONNECT_FILENAME = "sink.connect.filename";

    private final OceanusConfig oceanusConfig;

    /**
     * load config from Oceanus file.
     */
    public OceanusConfiguration() throws Exception {
        String path = formatPath();
        oceanusConfig = getOceanusConfigFromFile(path);
    }

    /**
     * fetch DEFAULT_CONFIG_FILE full path
     */
    private String formatPath() throws Exception {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        LOGGER.info("format first path {}", path);

        int index = path.indexOf(INLONG_MANAGER);
        if (index == -1) {
            throw new Exception(INLONG_MANAGER + " path not found in " + path);
        }

        path = path.substring(0, index);
        String confPath = path + INLONG_MANAGER + File.separator + "plugins" + File.separator + DEFAULT_CONFIG_FILE;
        File file = new File(confPath);
        if (!file.exists()) {
            String message = String.format("not found %s in path %s", DEFAULT_CONFIG_FILE, confPath);
            LOGGER.error(message);
            throw new Exception(message);
        }

        LOGGER.info("after format, {} located in {}", DEFAULT_CONFIG_FILE, confPath);
        return confPath;
    }

    /**
     * get oceanus config
     */
    public OceanusConfig getOceanusConfig() {
        return oceanusConfig;
    }

    /**
     * parse properties
     */
    private OceanusConfig getOceanusConfigFromFile(String fileName) throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        properties.load(bufferedReader);
        OceanusConfig oceanusConfig = new OceanusConfig();
        oceanusConfig.setOpenApi(properties.getProperty(OCEANUS_OPEN_API));
        oceanusConfig.setAuthUser(properties.getProperty(AUTH_USER));
        oceanusConfig.setAuthCmk(properties.getProperty(AUTH_CMK));
        oceanusConfig.setDefaultSortDistJarName(properties.getProperty(SORT_DIS_FILE_NAME));
        oceanusConfig.setDefaultSourceConnectName(properties.getProperty(SOURCE_CONNECT_FILE_NAME));
        oceanusConfig.setDefaultSinkConnectName(properties.getProperty(SINK_CONNECT_FILENAME));
        LOGGER.info("Test oceansunconfig={}", oceanusConfig);
        return oceanusConfig;
    }

}
