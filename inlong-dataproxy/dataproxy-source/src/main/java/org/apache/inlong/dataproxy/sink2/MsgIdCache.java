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

package org.apache.inlong.dataproxy.sink2;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.TimeUnit;

/**
 * 
 * Message ID sent cache
 */
public class MsgIdCache {

    private boolean enableDupCheck;
    private final LoadingCache<String, Long> msgIdCache;

    public MsgIdCache(boolean enableDupCheck,
            int visitConcurLevel, int initCacheCapacity, long expiredDurSec) {
        this.enableDupCheck = enableDupCheck;
        msgIdCache = CacheBuilder.newBuilder()
                .concurrencyLevel(visitConcurLevel)
                .initialCapacity(initCacheCapacity)
                .expireAfterAccess(expiredDurSec, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Long>() {

                    // default load function, when the get value is called,
                    // if the key does not have a corresponding value,
                    // this method is called to load.
                    @Override
                    public Long load(String key) {
                        return System.currentTimeMillis();
                    }
                });
    }

    public void invalidCache(String msgId) {
        if (msgId == null || !enableDupCheck) {
            return;
        }
        if (msgIdCache.asMap().containsKey(msgId)) {
            msgIdCache.invalidate(msgId);
        }
    }

    /**
     * Cache and judge whether the msg id exists
     *
     * @param msgId the message id need send message
     * @return Whether the message id has been sent
     */
    public boolean cacheIfAbsent(String msgId) {
        boolean duplicate = false;
        if (msgId == null || !enableDupCheck) {
            return duplicate;
        }
        if (msgIdCache.asMap().containsKey(msgId)) {
            duplicate = true;
        }
        msgIdCache.put(msgId, System.currentTimeMillis());
        return duplicate;
    }

    public long getCachedSize() {
        return msgIdCache.size();
    }

    public void clearMsgIdCache() {
        msgIdCache.cleanUp();
    }
}
