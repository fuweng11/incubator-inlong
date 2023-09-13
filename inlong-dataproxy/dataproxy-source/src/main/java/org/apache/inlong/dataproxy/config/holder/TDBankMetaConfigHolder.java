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
import org.apache.inlong.dataproxy.config.pojo.RmvDataItem;
import org.apache.inlong.dataproxy.config.pojo.TDBankMetaConfig;
import org.apache.inlong.dataproxy.consts.AttrConstants;

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
import java.util.ArrayList;
import java.util.Collections;
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

    private static final String metaConfigFileName = "tdbank_metadata.json";

    public static final String REMOVE_META_ITEMS = "removeMetaItems";
    public static final String REMOVE_ITEMS_KEY = "rmvItems";

    private static final int MAX_ALLOWED_JSON_FILE_SIZE = 300 * 1024 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(TDBankMetaConfigHolder.class);
    private static final Gson GSON = new Gson();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    // meta data
    private String tmpDataStr = "";
    private List<String> tmpMetaList = new ArrayList<>();
    private final AtomicLong lastSyncVersion = new AtomicLong(0);
    private String dataStr = "";
    private List<String> metaList = new ArrayList<>();
    private final Map<String, TDBankMetaConfig.ConfigItem> metaConfItemMap = new HashMap<>();
    private final AtomicLong lastUpdVersion = new AtomicLong(0);

    // cached data
    private final ConcurrentHashMap<String, String> bid2SrcTopicMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> bid2SrcMValueMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> bid2SinkTopicMap = new ConcurrentHashMap<>();

    public TDBankMetaConfigHolder() {
        super(metaConfigFileName);
    }

    /**
     * get source topic by groupId and streamId
     */
    public String getSrcTopicName(String groupId) {
        String topicName = null;
        if (StringUtils.isNotEmpty(groupId) && !bid2SrcTopicMap.isEmpty()) {
            topicName = bid2SrcTopicMap.get(groupId);
        }
        // if (LOG.isDebugEnabled()) {
        // LOG.debug("Get source topicName = {} by groupId = {}", topicName, groupId);
        // }
        return topicName;
    }

    /**
     * get sink topic by groupId and streamId
     */
    public String getSinkTopicName(String groupId) {
        String topicName = null;
        if (StringUtils.isNotEmpty(groupId) && !bid2SinkTopicMap.isEmpty()) {
            topicName = bid2SinkTopicMap.get(groupId);
        }
        // if (LOG.isDebugEnabled()) {
        // LOG.debug("Get sink topicName = {} by groupId = {}", topicName, groupId);
        // }
        return topicName;
    }

    public String getMxProperties(String groupId) {
        String mxValue = null;
        if (StringUtils.isNotEmpty(groupId) && !bid2SrcMValueMap.isEmpty()) {
            mxValue = bid2SrcMValueMap.get(groupId);
        }
        // if (LOG.isDebugEnabled()) {
        // LOG.debug("Get m properties = {} by groupId = {}", mxValue, groupId);
        // }
        return mxValue;
    }

    public boolean updateConfigMap(String inDataJsonStr) {
        if (StringUtils.isBlank(inDataJsonStr)) {
            return false;
        }
        TDBankMetaConfig metaConfig =
                GSON.fromJson(inDataJsonStr, TDBankMetaConfig.class);
        if (!metaConfig.isResult() || metaConfig.getErrCode() != 0) {
            return false;
        }
        List<String> result = getMetaStrList(metaConfig.getData());
        if (result.isEmpty()) {
            return false;
        }
        // check and update configure
        synchronized (this.lastSyncVersion) {
            if (this.lastSyncVersion.get() > this.lastUpdVersion.get()) {
                if (inDataJsonStr.equals(tmpDataStr) || !needAppend(this.tmpMetaList, result)) {
                    return false;
                }
                LOG.info("Load changed metadata {} , but reloading content, over {} ms",
                        getFileName(), System.currentTimeMillis() - this.lastSyncVersion.get());
                return false;
            }
            return storeConfigToFile(true, inDataJsonStr, result, metaConfig.getData());
        }
    }

    public boolean manualRmvMetaConfig(List<RmvDataItem> origRmvItems) {
        String key;
        List<String> rmvItemKeys = new ArrayList<>();
        LOG.info("manual remove meta-data: enter process!");
        for (RmvDataItem item : origRmvItems) {
            if (item == null
                    || !CommonConfigHolder.getInstance().getClusterIdSet().contains(item.getClusterId())
                    || StringUtils.isBlank(item.getBid())) {
                continue;
            }
            key = AttrConstants.SEPARATOR + item.getClusterId()
                    + AttrConstants.SEPARATOR + item.getBid().trim();
            rmvItemKeys.add(key);
        }
        if (rmvItemKeys.isEmpty()) {
            LOG.warn("Manual remove meta-data failure: no valid data to be deleted!");
            return false;
        }
        // check whether empty meta configure
        int count = 0;
        for (String rmvKey : rmvItemKeys) {
            if (this.metaConfItemMap.containsKey(rmvKey)) {
                count++;
            }
        }
        if (count >= this.metaConfItemMap.size()) {
            LOG.warn("Manual remove meta-data failure: not allowed to delete all meta-data!");
            return false;
        }
        // remove items
        for (String rmvKey : rmvItemKeys) {
            this.metaConfItemMap.remove(rmvKey);
        }
        LOG.info("manual remove meta-data: removed cached data process!");
        // build new Meta configiure
        TDBankMetaConfig metaConfig = new TDBankMetaConfig(this.metaConfItemMap.values());
        List<String> result = getMetaStrList(metaConfig.getData());
        if (result.isEmpty()) {
            LOG.warn("Manual remove meta-data failure: prepare store data is empty!");
            return false;
        }
        LOG.info("manual remove meta-data: prepare remove data set are {}", rmvItemKeys);
        String newDataJsonStr = GSON.toJson(metaConfig);
        // check and update configure
        synchronized (this.lastSyncVersion) {
            return storeConfigToFile(false, newDataJsonStr, result, metaConfig.getData());
        }
    }

    public Set<String> getAllSinkTopicName() {
        Set<String> result = new HashSet<>();
        // add default topics first
        if (CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
            result.addAll(CommonConfigHolder.getInstance().getDefTopics());
        }
        // add configured topics
        for (String topicName : bid2SinkTopicMap.values()) {
            if (StringUtils.isBlank(topicName)) {
                continue;
            }
            result.add(topicName);
        }
        return result;
    }

    @Override
    protected boolean loadFromFileToHolder() {
        if (!CommonConfigHolder.getInstance().isMetaInfoGetFromTDBank()) {
            LOG.warn("Get meta from Manager, not reload configure json from {}", getFileName());
            return true;
        }
        // check meta update setting
        if (!CommonConfigHolder.getInstance().isEnableStartupUsingLocalMetaFile()
                && !ConfigManager.handshakeManagerOk.get()) {
            LOG.warn("Failed to load json config from {}, don't obtain metadata from the Manager,"
                    + " and the startup via the cache file is false", getFileName());
            return false;
        }
        String jsonString = "";
        List<String> newDataList;
        readWriteLock.writeLock().lock();
        try {
            jsonString = loadConfigFromFile();
            if (StringUtils.isBlank(jsonString)) {
                LOG.warn("Load changed json {}, but no records configured", getFileName());
                return true;
            }
            TDBankMetaConfig metaConfig =
                    GSON.fromJson(jsonString, TDBankMetaConfig.class);
            if (!metaConfig.isResult() || metaConfig.getErrCode() != 0) {
                LOG.warn("Load failed json config from {}, result is {}, error code is {}",
                        getFileName(), metaConfig.isResult(), metaConfig.getErrCode());
                return true;
            }
            List<TDBankMetaConfig.ConfigItem> bidConfigs = metaConfig.getData();
            if (bidConfigs == null) {
                LOG.warn("Load failed json config from {}, malformed content, data is null", getFileName());
                return true;
            }
            if (bidConfigs.isEmpty()) {
                LOG.warn("Load failed json config from {}, malformed content, data is empty!", getFileName());
                return true;
            }
            newDataList = getMetaStrList(bidConfigs);
            if (newDataList.isEmpty()) {
                LOG.warn("Load json config from {} failed, no valid topic record!", getFileName());
                return true;
            }
            // update cache data
            if (jsonString.equals(this.dataStr) || !needAppend(this.metaList, newDataList)) {
                LOG.warn("Load json config from {}, no new records found!", getFileName());
                return true;
            }
            if (updateCacheData(jsonString, newDataList, metaConfig)) {
                LOG.info("Load changed {} file success!", getFileName());
            }
            return true;
        } catch (Throwable e) {
            LOG.warn("Process json {} changed data {} failure", getFileName(), jsonString, e);
            return false;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private boolean updateCacheData(String newConfigStr,
            List<String> newDataList, TDBankMetaConfig metaConfig) {
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
                    || !CommonConfigHolder.getInstance().getClusterIdSet().contains(item.getClusterId())
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
                    getFileName(), CommonConfigHolder.getInstance().getClusterIdsStr());
            return false;
        }
        // add new bid2SrcTopic config
        bid2SrcTopicMap.putAll(tmpBidTopicMap);
        bid2SinkTopicMap.putAll(tmpBidTopicMap);
        // add new mq cluster config
        bid2SrcMValueMap.putAll(tmpBidMValueMap);
        if (this.lastSyncVersion.get() == 0) {
            this.lastUpdVersion.set(System.currentTimeMillis());
            this.lastSyncVersion.compareAndSet(0, this.lastUpdVersion.get());
        } else {
            this.lastUpdVersion.set(this.lastSyncVersion.get());
        }
        this.metaList = newDataList;
        this.dataStr = newConfigStr;
        repCachedConfigItems(bidConfigs);
        return true;
    }

    /**
     * store meta config to file
     */
    private boolean storeConfigToFile(boolean onlyAppend, String metaJsonStr,
            List<String> result, List<TDBankMetaConfig.ConfigItem> configItemList) {
        boolean isSuccess = false;
        String filePath = getFilePath();
        if (StringUtils.isBlank(filePath)) {
            LOG.error("Error in writing file {} as the file path is null.", getFileName());
            return isSuccess;
        }
        readWriteLock.writeLock().lock();
        try {
            // prepare update
            String newMetaJsonStr;
            if (onlyAppend) {
                if (metaJsonStr.equals(dataStr) || !needAppend(this.metaList, result)) {
                    return false;
                }
                newMetaJsonStr = appendUpdatedMetaConfig(configItemList);
            } else {
                newMetaJsonStr = metaJsonStr;
            }
            // process store file
            File sourceFile = new File(filePath);
            File targetFile = new File(getNextBackupFileName());
            File tmpNewFile = new File(getFileName() + ".tmp");

            if (sourceFile.exists()) {
                FileUtils.copyFile(sourceFile, targetFile);
            }
            FileUtils.writeStringToFile(tmpNewFile, newMetaJsonStr, StandardCharsets.UTF_8);
            FileUtils.copyFile(tmpNewFile, sourceFile);
            tmpNewFile.delete();
            this.tmpDataStr = metaJsonStr;
            this.tmpMetaList = result;
            this.lastSyncVersion.set(System.currentTimeMillis());
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

    private List<String> getMetaStrList(List<TDBankMetaConfig.ConfigItem> bidConfigs) {
        List<String> result = new ArrayList<>();
        if (bidConfigs == null || bidConfigs.isEmpty()) {
            return result;
        }
        String key;
        for (TDBankMetaConfig.ConfigItem item : bidConfigs) {
            if (item == null
                    || !CommonConfigHolder.getInstance().getClusterIdSet().contains(item.getClusterId())
                    || StringUtils.isBlank(item.getBid())
                    || StringUtils.isBlank(item.getTopic())) {
                continue;
            }
            key = AttrConstants.SEPARATOR + item.getClusterId()
                    + AttrConstants.SEPARATOR + item.getBid().trim()
                    + AttrConstants.SEPARATOR + item.getTopic().trim()
                    + AttrConstants.SEPARATOR + item.getM().trim();
            if (result.contains(key)) {
                continue;
            }
            result.add(key);
        }
        Collections.sort(result);
        return result;
    }

    private String appendUpdatedMetaConfig(List<TDBankMetaConfig.ConfigItem> bidConfigs) {
        String key;
        for (TDBankMetaConfig.ConfigItem item : bidConfigs) {
            if (item == null
                    || !CommonConfigHolder.getInstance().getClusterIdSet().contains(item.getClusterId())
                    || StringUtils.isBlank(item.getBid())
                    || StringUtils.isBlank(item.getTopic())) {
                continue;
            }
            key = AttrConstants.SEPARATOR + item.getClusterId()
                    + AttrConstants.SEPARATOR + item.getBid().trim();
            this.metaConfItemMap.put(key, item);
        }
        TDBankMetaConfig metaConfig = new TDBankMetaConfig(this.metaConfItemMap.values());
        return GSON.toJson(metaConfig);
    }

    private void repCachedConfigItems(List<TDBankMetaConfig.ConfigItem> bidConfigs) {
        String key;
        this.metaConfItemMap.clear();
        for (TDBankMetaConfig.ConfigItem item : bidConfigs) {
            if (item == null
                    || !CommonConfigHolder.getInstance().getClusterIdSet().contains(item.getClusterId())
                    || StringUtils.isBlank(item.getBid())
                    || StringUtils.isBlank(item.getTopic())) {
                continue;
            }
            key = AttrConstants.SEPARATOR + item.getClusterId()
                    + AttrConstants.SEPARATOR + item.getBid().trim();
            this.metaConfItemMap.put(key, item);
        }
    }

    private boolean needAppend(List<String> curList, List<String> newList) {
        if (curList.equals(newList)) {
            return false;
        }
        for (String item : newList) {
            if (!curList.contains(item)) {
                return true;
            }
        }
        return false;
    }
}
