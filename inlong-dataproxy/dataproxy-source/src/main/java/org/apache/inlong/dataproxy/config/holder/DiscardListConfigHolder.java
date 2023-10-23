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

import org.apache.inlong.dataproxy.config.ConfigHolder;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * save need to discard groupId#streamId list
 */
public class DiscardListConfigHolder extends ConfigHolder {

    private static final Logger LOG = LoggerFactory.getLogger(DiscardListConfigHolder.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final String discardlistFileName = "discardlist.properties";
    private static final String SEP_GROUPID_STREAMID = "#";
    private final ConcurrentHashMap<String, Long> confHolder = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> groupHolder = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> groupStreamHolder =
            new ConcurrentHashMap<>();

    public DiscardListConfigHolder() {
        super(discardlistFileName);
    }

    @Override
    protected boolean loadFromFileToHolder() {
        readWriteLock.writeLock().lock();
        try {
            Map<String, Long> tmpHolder = loadFile();
            if (tmpHolder == null) {
                return true;
            }
            // clear removed records
            Set<String> rmvTmpKeys = new HashSet<>();
            for (Map.Entry<String, Long> entry : confHolder.entrySet()) {
                if (!tmpHolder.containsKey(entry.getKey())) {
                    rmvTmpKeys.add(entry.getKey());
                }
            }
            String[] itemKeys;
            String tmpGroupId;
            ConcurrentHashMap<String, Long> streamIdMap;
            boolean removed = false;
            for (String tmpKey : rmvTmpKeys) {
                removed = true;
                confHolder.remove(tmpKey);
                itemKeys = tmpKey.split(SEP_GROUPID_STREAMID);
                if (StringUtils.isBlank(itemKeys[0])) {
                    continue;
                }
                tmpGroupId = itemKeys[0].trim();
                if (itemKeys.length == 1) {
                    groupHolder.remove(tmpGroupId);
                } else {
                    if (StringUtils.isBlank(itemKeys[1])) {
                        groupHolder.remove(tmpGroupId);
                    } else {
                        streamIdMap = groupStreamHolder.get(tmpGroupId);
                        if (streamIdMap == null) {
                            continue;
                        }
                        streamIdMap.remove(itemKeys[1].trim());
                        if (streamIdMap.isEmpty()) {
                            groupStreamHolder.remove(tmpGroupId);
                        }
                    }
                }
            }
            // add updated records
            boolean added = false;
            Set<String> addedTmpKeys = new HashSet<>();
            Set<String> illegalTmpKeys = new HashSet<>();
            for (Map.Entry<String, Long> entry : tmpHolder.entrySet()) {
                if (entry == null || StringUtils.isBlank(entry.getKey())) {
                    continue;
                }
                if (confHolder.containsKey(entry.getKey())) {
                    continue;
                }
                confHolder.put(entry.getKey(), entry.getValue());
                itemKeys = entry.getKey().split(SEP_GROUPID_STREAMID);
                if (StringUtils.isBlank(itemKeys[0])) {
                    illegalTmpKeys.add(entry.getKey());
                    continue;
                }
                tmpGroupId = itemKeys[0].trim();
                if (itemKeys.length == 1) {
                    groupHolder.put(tmpGroupId, entry.getValue());
                } else {
                    if (StringUtils.isBlank(itemKeys[1])) {
                        groupHolder.put(tmpGroupId, entry.getValue());
                    } else {
                        streamIdMap = groupStreamHolder.get(tmpGroupId);
                        if (streamIdMap == null) {
                            streamIdMap = new ConcurrentHashMap<>();
                            streamIdMap.put(itemKeys[1].trim(), entry.getValue());
                            groupStreamHolder.put(tmpGroupId, streamIdMap);
                        } else {
                            streamIdMap.put(itemKeys[1].trim(), entry.getValue());
                        }
                    }
                }
                added = true;
                addedTmpKeys.add(entry.getKey());
            }
            // output load result
            if (added || removed || !illegalTmpKeys.isEmpty()) {
                LOG.info("Load DiscardList data, added {}, deleted {}, illegal items {}",
                        addedTmpKeys, rmvTmpKeys, illegalTmpKeys);
            }
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public boolean isDiscardInLongID(String groupId, String streamId) {
        if (confHolder.isEmpty() || StringUtils.isEmpty(groupId)) {
            return false;
        }
        if (StringUtils.isNotEmpty(streamId)) {
            Map<String, Long> streamIdMap = groupStreamHolder.get(groupId);
            if (streamIdMap != null && streamIdMap.containsKey(streamId)) {
                return true;
            }
        }
        return groupHolder.containsKey(groupId);
    }

    private Map<String, Long> loadFile() {
        String filePath = getFilePath();
        if (StringUtils.isBlank(filePath)) {
            LOG.error("Fail to load " + getFileName() + " as the file path is empty");
            return null;
        }
        FileReader reader = null;
        BufferedReader br = null;
        Map<String, Long> configMap = new HashMap<>();
        try {
            String line;
            reader = new FileReader(filePath);
            br = new BufferedReader(reader);
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isBlank(line)
                        || line.startsWith("#")
                        || line.startsWith(";")) {
                    continue;
                }
                configMap.put(line, System.currentTimeMillis());
            }
            return configMap;
        } catch (Throwable e) {
            LOG.error("Fail to load " + getFileName() + ", path = {}, and e = {}", filePath, e);
            return null;
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(br);
        }
    }
}
