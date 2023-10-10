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

import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.AbstractChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BufferQueueChannel
 */
public class BufferQueueChannel extends AbstractChannel {

    public static final Logger LOG = LoggerFactory.getLogger(BufferQueueChannel.class);

    public static final String KEY_MAX_BUFFERQUEUE_COUNT = "maxBufferQueueCount";
    // average message size is 1KB.
    public static final int DEFAULT_MAX_BUFFERQUEUE_COUNT = 128 * 1024;
    public static final String KEY_MAX_BUFFERQUEUE_SIZE_KB = "maxBufferQueueSizeKb";
    public static final int DEFAULT_MAX_BUFFERQUEUE_SIZE_KB = 128 * 1024;
    public static final String KEY_RELOADINTERVAL = "reloadInterval";

    private Context context;
    private String cachedName;
    private int maxBufferQueueCount;
    private Semaphore countSemaphore;
    private int maxBufferQueueSizeKb;
    private BufferQueue<Event> bufferQueue;
    private ThreadLocal<ProxyTransaction> currentTransaction = new ThreadLocal<>();
    protected Timer channelTimer;
    private AtomicLong takeCounter = new AtomicLong(0);
    private AtomicLong putCounter = new AtomicLong(0);
    private AtomicLong putFailCounter = new AtomicLong(0);

    /**
     * Constructor
     */
    public BufferQueueChannel() {
    }

    /**
     * put
     *
     * @param  event
     * @throws ChannelException
     */
    @Override
    public void put(Event event) throws ChannelException {
        // count size
        int eventCount;
        int eventSize;
        if (event instanceof ProxyPackEvent) {
            ProxyPackEvent packEvent = (ProxyPackEvent) event;
            eventCount = packEvent.getEvents().size();
            eventSize = 0;
            for (ProxyEvent e : packEvent.getEvents()) {
                eventSize += e.getBody().length;
            }
        } else {
            eventCount = 1;
            eventSize = event.getBody().length;
        }
        putCounter.addAndGet(eventCount);
        if (this.countSemaphore.tryAcquire(eventCount)) {
            if (this.bufferQueue.tryAcquire(eventSize)) {
                ProxyTransaction transaction = currentTransaction.get();
                Preconditions.checkState(transaction != null, "No transaction exists for this thread");
                transaction.doPut(event);
            } else {
                this.countSemaphore.release(eventCount);
                this.putFailCounter.addAndGet(eventCount);
                throw new ChannelException(
                        "Put queue for BufferQueue channel of maxBufferQueueSizeKb capacity " +
                                maxBufferQueueSizeKb + " full, consider committing more frequently, " +
                                "increasing maxBufferQueueSizeKb count");
            }
        } else {
            this.putFailCounter.addAndGet(eventCount);
            throw new ChannelException(
                    "Put queue for BufferQueue channel of maxBufferQueueCount capacity " +
                            maxBufferQueueCount + " full, consider committing more frequently, " +
                            "increasing maxBufferQueueCount count");
        }
    }

    /**
     * take
     *
     * @return Event
     * @throws ChannelException
     */
    @Override
    public Event take() throws ChannelException {
        Event event = this.bufferQueue.pollRecord();
        if (event != null) {
            ProxyTransaction transaction = currentTransaction.get();
            if (event instanceof ProxyPackEvent) {
                transaction.doTake(event);
                takeCounter.addAndGet(((ProxyPackEvent) event).getEvents().size());

            } else {
                Preconditions.checkState(transaction != null, "No transaction exists for this thread");
                transaction.doTake(event);
                takeCounter.incrementAndGet();
            }
        }
        return event;
    }

    /**
     * getTransaction
     *
     * @return new transaction
     */
    @Override
    public Transaction getTransaction() {
        ProxyTransaction newTransaction = new ProxyTransaction(this.countSemaphore, this.bufferQueue);
        this.currentTransaction.set(newTransaction);
        return newTransaction;
    }

    /**
     * start
     */
    @Override
    public void start() {
        super.start();
        try {
            this.setReloadTimer();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * setReloadTimer
     */
    protected void setReloadTimer() {
        channelTimer = new Timer(true);
        long reloadInterval = context.getLong(KEY_RELOADINTERVAL, 60000L);
        TimerTask channelTask = new TimerTask() {

            public void run() {
                LOG.info("{} status check: queueSize={},availablePermits={},maxBufferQueueCount={},"
                        + "availablePermits={},put={},take={},putFail={}",
                        cachedName, bufferQueue.size(), bufferQueue.availablePermits(),
                        maxBufferQueueCount, countSemaphore.availablePermits(),
                        putCounter.getAndSet(0), takeCounter.getAndSet(0),
                        putFailCounter.getAndSet(0));
            }
        };
        channelTimer.schedule(channelTask,
                new Date(System.currentTimeMillis() + reloadInterval),
                reloadInterval);
    }

    /**
     * configure
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.context = context;
        this.cachedName = getName();
        this.maxBufferQueueCount = context.getInteger(KEY_MAX_BUFFERQUEUE_COUNT, DEFAULT_MAX_BUFFERQUEUE_COUNT);
        this.countSemaphore = new Semaphore(maxBufferQueueCount, true);
        this.maxBufferQueueSizeKb = context.getInteger(KEY_MAX_BUFFERQUEUE_SIZE_KB, DEFAULT_MAX_BUFFERQUEUE_SIZE_KB);
        this.bufferQueue = new BufferQueue<>(maxBufferQueueSizeKb);
    }
}
