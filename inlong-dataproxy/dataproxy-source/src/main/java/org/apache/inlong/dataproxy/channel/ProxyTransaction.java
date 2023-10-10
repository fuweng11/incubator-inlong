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

package org.apache.inlong.dataproxy.channel;

import org.apache.inlong.dataproxy.utils.BufferQueue;
import org.apache.inlong.sdk.commons.protocol.ProxyEvent;
import org.apache.inlong.sdk.commons.protocol.ProxyPackEvent;

import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * ProfileTransaction
 */
public class ProxyTransaction implements Transaction {

    public static final Logger LOG = LoggerFactory.getLogger(ProxyTransaction.class);

    private Semaphore countSemaphore;
    private BufferQueue<Event> bufferQueue;
    private List<Event> takeList = new ArrayList<>();
    private List<Event> putList = new ArrayList<>();

    /**
     * Constructor
     *
     * @param countSemaphore
     * @param bufferQueue
     */
    public ProxyTransaction(Semaphore countSemaphore, BufferQueue<Event> bufferQueue) {
        this.countSemaphore = countSemaphore;
        this.bufferQueue = bufferQueue;
    }

    /**
     * begin
     */
    @Override
    public void begin() {
    }

    /**
     * commit
     */
    @Override
    public void commit() {
        for (Event event : takeList) {
            if (event instanceof ProxyPackEvent) {
                ProxyPackEvent packEvent = (ProxyPackEvent) event;
                int eventCount = packEvent.getEvents().size();
                int eventSize = 0;
                for (ProxyEvent e : packEvent.getEvents()) {
                    eventSize += e.getBody().length;
                }
                countSemaphore.release(eventCount);
                bufferQueue.release(eventSize);
            } else {
                countSemaphore.release();
                bufferQueue.release(event.getBody().length);
            }
        }
        this.takeList.clear();
        for (Event event : putList) {
            this.bufferQueue.offer(event);
        }
        this.putList.clear();
    }

    /**
     * rollback
     */
    @Override
    public void rollback() {
        for (Event event : takeList) {
            this.bufferQueue.offer(event);
        }
        this.takeList.clear();
        for (Event event : putList) {
            if (event instanceof ProxyPackEvent) {
                ProxyPackEvent packEvent = (ProxyPackEvent) event;
                int eventCount = packEvent.getEvents().size();
                int eventSize = 0;
                for (ProxyEvent e : packEvent.getEvents()) {
                    eventSize += e.getBody().length;
                }
                countSemaphore.release(eventCount);
                bufferQueue.release(eventSize);
            } else {
                countSemaphore.release();
                bufferQueue.release(event.getBody().length);
            }
        }
        this.putList.clear();
    }

    /**
     * close
     */
    @Override
    public void close() {
    }

    /**
     * doTake
     *
     * @param event
     */
    public void doTake(Event event) {
        this.takeList.add(event);
    }

    /**
     * doPut
     *
     * @param event
     */
    public void doPut(Event event) {
        this.putList.add(event);
    }
}
