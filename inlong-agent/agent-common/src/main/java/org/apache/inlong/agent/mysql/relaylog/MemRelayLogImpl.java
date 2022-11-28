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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class MemRelayLogImpl implements RelayLog {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final long maxDataSize;
    private LinkedList<byte[]> dataList;
    private AtomicLong containDataLength;
    private AtomicBoolean bRunning;
    private ReentrantLock reenLock;

    public MemRelayLogImpl(long maxDataSize) {
        this.maxDataSize = maxDataSize;
        dataList = new LinkedList<byte[]>();
        bRunning = new AtomicBoolean(true);
        containDataLength = new AtomicLong(0L);
        reenLock = new ReentrantLock();
    }

    @Override
    public boolean putLog(byte[] bytes) {

        if (bytes == null || bytes.length <= 0) {
            return true;
        }

        while ((containDataLength.get() + bytes.length) > maxDataSize && bRunning.get()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }
        try {
            reenLock.lock();
            dataList.add(bytes);
        } finally {
            reenLock.unlock();
        }

        containDataLength.addAndGet(bytes.length);
        return true;
    }

    @Override
    public byte[] getLog() {

        byte[] bytes = null;
        try {
            reenLock.lock();
            bytes = dataList.poll();
        } finally {
            reenLock.unlock();
        }
        if (bytes != null) {
            long delSize = bytes.length * -1;
            containDataLength.addAndGet(delSize);
        }

        return bytes;
    }

    @Override
    public void close() {
        bRunning.set(false);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void clearLog() {
        try {
            reenLock.lock();
            bRunning.set(true);
            dataList = new LinkedList<byte[]>();
            containDataLength.set(0L);
        } finally {
            reenLock.unlock();
        }
    }

    @Override
    public void report() {
        logger.debug("dataList size : " + dataList.size() + " , data size : " + containDataLength.get() / 1024 / 1024);
    }

}
