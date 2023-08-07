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

package org.apache.inlong.dataproxy.config.holder;

import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.TDBankMetaConfig;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Json to object
 */
public class TDBankMetaConfigHolder extends ConfigHolder {

    private static final String metaConfigFileName = "metadata.json";

    private static final int MAX_ALLOWED_JSON_FILE_SIZE = 300 * 1024 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(TDBankMetaConfigHolder.class);
    private static final Gson GSON = new Gson();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    // meta data
    private String dataStr = "";
    private String tmpDataStr = "";
    private final AtomicLong lastSyncTime = new AtomicLong(0);
    // cached data
    private final ConcurrentHashMap<String, String> bid2TopicMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> bid2MValueMap = new ConcurrentHashMap<>();

    public TDBankMetaConfigHolder() {
        super(metaConfigFileName);
    }

    /**
     * get topic by groupId and streamId
     */
    public String getTopicName(String groupId) {
        String topicName = null;
        if (StringUtils.isNotEmpty(groupId) && !bid2TopicMap.isEmpty()) {
            topicName = bid2TopicMap.get(groupId);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get topicName = {} by groupId = {}", topicName, groupId);
        }
        return topicName;
    }

    public String getMxProperties(String groupId) {
        String mxValue = null;
        if (StringUtils.isNotEmpty(groupId) && !bid2MValueMap.isEmpty()) {
            mxValue = bid2MValueMap.get(groupId);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get m properties = {} by groupId = {}", mxValue, groupId);
        }
        return mxValue;
    }

    public boolean updateConfigMap(String inDataJsonStr) {
        if (StringUtils.isBlank(inDataJsonStr)
                || inDataJsonStr.equals(this.tmpDataStr)) {
            return false;
        }
        if (storeConfigToFile(inDataJsonStr)) {
            this.tmpDataStr = inDataJsonStr;
            lastSyncTime.set(System.currentTimeMillis());
            return true;
        }
        return false;
    }

    public Map<String, String> forkCachedBidTopicConfig() {
        Map<String, String> result = new HashMap<>();
        if (bid2TopicMap.isEmpty()) {
            return result;
        }
        for (Map.Entry<String, String> entry : bid2TopicMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public Set<String> getAllTopicName() {
        Set<String> result = new HashSet<>();
        // add default topics first
        if (CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
            result.addAll(CommonConfigHolder.getInstance().getDefTopics());
        }
        // add configured topics
        for (String topicName : bid2TopicMap.values()) {
            if (StringUtils.isBlank(topicName)) {
                continue;
            }
            result.add(topicName);
        }
        return result;
    }

    @Override
    protected boolean loadFromFileToHolder() {
        String jsonString = "";
        if (!CommonConfigHolder.getInstance().isEnableTDBankLogic()) {
            return true;
        }
        readWriteLock.readLock().lock();
        try {
            jsonString = loadConfigFromFile();
            if (StringUtils.isBlank(jsonString)) {
                LOG.info("Load changed json {}, but no records configured", getFileName());
                return false;
            }
            TDBankMetaConfig metaConfig =
                    GSON.fromJson(jsonString, TDBankMetaConfig.class);
            if (!metaConfig.isResult() || metaConfig.getErrCode() != 0) {
                LOG.warn("Load failed json config from {}, result is {}, error code is {}",
                        getFileName(), metaConfig.isResult(), metaConfig.getErrCode());
                return false;
            }
            List<TDBankMetaConfig.ConfigItem> bidConfigs = metaConfig.getData();
            if (bidConfigs == null) {
                LOG.warn("Load failed json config from {}, malformed content, data is null", getFileName());
                return false;
            }
            if (bidConfigs.isEmpty()) {
                LOG.warn("Load failed json config from {}, malformed content, data is empty!", getFileName());
                return false;
            }
            if (!CommonConfigHolder.getInstance().isEnableStartupUsingLocalMetaFile()
                    && !ConfigManager.handshakeManagerOk.get()) {
                LOG.info("Failed to load json config from {}, don't obtain metadata from the Manager,"
                        + " and the startup via the cache file is false", getFileName());
                return false;
            }
            // update cache data
            if (updateCacheData(metaConfig)) {
                // update cache string
                this.lastSyncTime.set(System.currentTimeMillis());
                this.dataStr = jsonString;
                this.tmpDataStr = jsonString;
                LOG.info("Load changed json {}, loaded data {}, updated cache ({}, {})",
                        getFileName(), dataStr, bid2TopicMap, bid2MValueMap);
                return true;
            }
            return false;
        } catch (Throwable e) {
            //
            LOG.info("Process json {} changed data {} failure", getFileName(), jsonString, e);
            return false;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private boolean updateCacheData(TDBankMetaConfig metaConfig) {
        // get and valid bid-topic configure
        List<TDBankMetaConfig.ConfigItem> bidConfigs = metaConfig.getData();
        if (bidConfigs == null) {
            LOG.warn("Load failed json config from {}, malformed content, proxyCluster field is null",
                    getFileName());
            return false;
        }
        String tmpBid;
        Map<String, String> tmpBidTopicMap = new HashMap<>();
        Map<String, String> tmpBidMValueMap = new HashMap<>();
        for (TDBankMetaConfig.ConfigItem item : bidConfigs) {
            if (item == null
                    || item.getClusterId() != CommonConfigHolder.getInstance().getClusterId()
                    || StringUtils.isBlank(item.getBid())
                    || StringUtils.isBlank(item.getTopic())) {
                continue;
            }
            tmpBid = item.getBid().trim();
            tmpBidTopicMap.put(tmpBid, item.getTopic().trim());
            if (StringUtils.isNotBlank(item.getM())) {
                tmpBidMValueMap.put(tmpBid, "m=" + item.getM().trim());
            }
        }
        if (tmpBidTopicMap.isEmpty()) {
            LOG.warn("Load failed json config from {}, no valid bid-topic metaConfig for clusterId {}",
                    getFileName(), CommonConfigHolder.getInstance().getClusterId());
            return false;
        }
        // remove deleted bid2topic config
        Set<String> tmpKeys = new HashSet<>();
        for (Map.Entry<String, String> entry : bid2TopicMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (!tmpBidTopicMap.containsKey(entry.getKey())) {
                tmpKeys.add(entry.getKey());
            }
        }
        for (String key : tmpKeys) {
            bid2TopicMap.remove(key);
        }
        // add new bid2topic config
        bid2TopicMap.putAll(tmpBidTopicMap);
        // remove deleted cluster config
        tmpKeys.clear();
        for (Map.Entry<String, String> entry : bid2MValueMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (!tmpBidMValueMap.containsKey(entry.getKey())) {
                tmpKeys.add(entry.getKey());
            }
        }
        for (String key : tmpKeys) {
            bid2MValueMap.remove(key);
        }
        // add new mq cluster config
        bid2MValueMap.putAll(tmpBidMValueMap);
        // callback meta configure updated
        executeCallbacks();
        return true;
    }

    /**
     * store meta config to file
     */
    private boolean storeConfigToFile(String metaJsonStr) {
        boolean isSuccess = false;
        String filePath = getFilePath();
        if (StringUtils.isBlank(filePath)) {
            LOG.error("Error in writing file {} as the file path is null.", getFileName());
            return isSuccess;
        }
        readWriteLock.writeLock().lock();
        try {
            File sourceFile = new File(filePath);
            File targetFile = new File(getNextBackupFileName());
            File tmpNewFile = new File(getFileName() + ".tmp");

            if (sourceFile.exists()) {
                FileUtils.copyFile(sourceFile, targetFile);
            }
            FileUtils.writeStringToFile(tmpNewFile, metaJsonStr, StandardCharsets.UTF_8);
            FileUtils.copyFile(tmpNewFile, sourceFile);
            tmpNewFile.delete();
            isSuccess = true;
            setFileChanged();
        } catch (Throwable ex) {
            LOG.error("Error in writing file {}", getFileName(), ex);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return isSuccess;
    }

    /**
     * load from holder
     */
    private String loadConfigFromFile() {
        String result = "";
        if (StringUtils.isBlank(getFileName())) {
            LOG.error("Fail to load json {} as the file name is null.", getFileName());
            return result;
        }
        InputStream inStream = null;
        try {
            URL url = getClass().getClassLoader().getResource(getFileName());
            inStream = url != null ? url.openStream() : null;
            if (inStream == null) {
                LOG.error("Fail to load json {} as the input stream is null", getFileName());
                return result;
            }
            int size = inStream.available();
            if (size > MAX_ALLOWED_JSON_FILE_SIZE) {
                LOG.error("Fail to load json {} as the content size({}) over max allowed size({})",
                        getFileName(), size, MAX_ALLOWED_JSON_FILE_SIZE);
                return result;
            }
            byte[] buffer = new byte[size];
            inStream.read(buffer);
            result = new String(buffer, StandardCharsets.UTF_8);
        } catch (Throwable e) {
            LOG.error("Fail to load json {}", getFileName(), e);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOG.error("Fail in inStream.close for file {}", getFileName(), e);
                }
            }
        }
        return result;
    }
}
