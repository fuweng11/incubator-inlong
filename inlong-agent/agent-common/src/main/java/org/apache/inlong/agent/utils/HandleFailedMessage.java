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

import org.apache.inlong.agent.entites.ProxyEvent;
import org.apache.inlong.sdk.dataproxy.FileCallback;
import org.apache.inlong.sdk.dataproxy.SendResult;
import org.apache.inlong.sdk.dataproxy.network.HttpProxySender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class HandleFailedMessage {

    public static final Logger LOG = LogManager.getLogger(HandleFailedMessage.class);
    private final LinkedBlockingQueue<ProxyEvent> msgQueue = new LinkedBlockingQueue<ProxyEvent>();
    private final HandleFailedMessageThread handle = new HandleFailedMessageThread();
    private boolean stopFlag = false;
    private HttpProxySender sender;

    private HandleFailedMessage() {
        handle.setDaemon(true);
        handle.start();
    }

    public static HandleFailedMessage getInstance() {
        return HandleFailedMessageHolder.INSTANCE;
    }

    public LinkedBlockingQueue<ProxyEvent> getMsgQueue() {
        return msgQueue;
    }

    public void setSender(HttpProxySender sender) {
        this.sender = sender;
    }

    public void stop() {
        stopFlag = true;
        handle.interrupt();
    }

    private static class HandleFailedMessageHolder {

        private static final HandleFailedMessage INSTANCE = new HandleFailedMessage();
    }

    public static class BusMessageQueueCallback extends FileCallback {

        public static final Logger LOG = LogManager.getLogger(BusMessageQueueCallback.class);
        private ProxyEvent event;// message content
        private HandleFailedMessage handler = HandleFailedMessage.getInstance();

        public BusMessageQueueCallback() {
        }

        public BusMessageQueueCallback(ProxyEvent event) {
            super();
            this.event = event;
        }

        @Override
        public void onMessageAck(String result) {
            LOG.info("onMessageAck return result = " + result);
        }

        @Override
        public void onMessageAck(SendResult result) {
            if (result == SendResult.ASYNC_CALLBACK_BUFFER_FULL) {
                LOG.warn("this buffer is full");
                addBackToQueue();
            } else if (result == SendResult.NO_CONNECTION) {
                LOG.warn("SendResult.NO_CONNECTION");
                addBackToQueue();
            } else if (result == SendResult.CONNECTION_BREAK) {
                LOG.warn("SendResult.CONNECTION_BREAK");
                addBackToQueue();
            } else if (result == SendResult.THREAD_INTERRUPT) {
                LOG.warn("SendResult.THREAD_INTERRUPT");
                addBackToQueue();
            } else if (result == SendResult.TIMEOUT) {
                LOG.warn("SendResult.TIMEOUT!");
                addBackToQueue();
            } else if (result == SendResult.INVALID_ATTRIBUTES) {
                LOG.warn("the attributes is invalid ,please check !");
            } else {
                LOG.warn("unknown warn! {}", result);
                addBackToQueue();
            }
        }

        private void addBackToQueue() {
            try {
                handler.getMsgQueue().put(event);
            } catch (InterruptedException e) {
                LOG.warn("Put failed message task to queue failed ", e);
            }
        }

    }

    class HandleFailedMessageThread extends Thread {

        @Override
        public void run() {
            while (!stopFlag) {
                if (!msgQueue.isEmpty()) {
                    ProxyEvent event = msgQueue.poll();
                    BusMessageQueueCallback callback = new BusMessageQueueCallback(event);
                    sender.asyncSendMessage(event.getMessage(),
                            event.getBid(), event.getTid(), event.getTime(), event.getTimeout(),
                            TimeUnit.MILLISECONDS, callback);
                }
                try {
                    Thread.sleep(5000);
                } catch (Throwable e) {
                    LOG.warn("handle failed message is interrupted");
                }
            }
        }
    }

}
