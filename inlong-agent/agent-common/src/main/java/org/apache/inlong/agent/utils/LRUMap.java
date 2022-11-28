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

package org.apache.inlong.agent.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUMap<K, V> extends LinkedHashMap<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LRUMap.class);

    private static final long serialVersionUID = -2370626402731464520L;
    private final ConcurrentHashMap<Object, Long> timeStamp = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private long outTimeSeconds = 60 * 1000L;

    public LRUMap(long outTimeSeconds) {
        this(outTimeSeconds, 16, 0.75f);
    }

    public LRUMap(long outTimeSeconds, int initialCapacity) {
        this(outTimeSeconds, initialCapacity, 0.75f);
    }

    public LRUMap(long outTimeSeconds, int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        if (outTimeSeconds > 0) {
            this.outTimeSeconds = outTimeSeconds;
        }
        initClear();
    }

    @Override
    public V get(Object key) {

        if (key == null) {
            return null;
        }

        rwLock.readLock().lock();
        try {
            V obj = super.get(key);
            if (obj != null) {
                timeStamp.put(key, System.currentTimeMillis());
            }
            return obj;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            return false;
        }

        rwLock.readLock().lock();
        try {
            return super.containsKey(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) {
            return null;
        }

        rwLock.writeLock().lock();
        try {
            V obj = null;

            timeStamp.put(key, System.currentTimeMillis());
            obj = super.put(key, value);
            return obj;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        rwLock.writeLock().lock();
        try {
            timeStamp.clear();
            super.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean removeValue(V val) {
        rwLock.writeLock().lock();
        boolean removed = false;
        try {
            Iterator<Map.Entry<K, V>> iterator = this.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<K, V> entry = iterator.next();
                if (Objects.equals(val, entry.getValue())) {
                    timeStamp.remove(entry.getKey());
                    iterator.remove();
                    removed = true;
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
        return removed;

    }

    @Override
    public V remove(Object key) {
        rwLock.writeLock().lock();
        try {
            V obj = super.remove(key);
            if (obj != null) {
                timeStamp.remove(key);
            }
            return obj;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // not thread safe
    private void removeInter(Object key) {
        this.remove(key);
        timeStamp.remove(key);
    }

    private void initClear() {
        Thread clear = new Thread(() -> {
            ArrayList<Object> needClearArr = new ArrayList<>();
            while (true) {
                try {
                    TimeUnit.MILLISECONDS.sleep(outTimeSeconds);

                    long outTimeStample = System.currentTimeMillis() - outTimeSeconds;
                    for (Map.Entry<Object, Long> entry : timeStamp.entrySet()) {
                        if (entry.getValue() < outTimeStample) {
                            needClearArr.add(entry.getKey());
                        }
                    }

                    rwLock.writeLock().lock();
                    try {
                        for (Object key : needClearArr) {
                            removeInter(key);
                        }
                    } finally {
                        rwLock.writeLock().unlock();
                    }
                    needClearArr.clear();
                } catch (Throwable t) {
                    LOGGER.error("", t);
                }
            }
        });
        clear.setName("LRUMap-Clear");
        clear.setDaemon(true);
        clear.start();
    }
}
