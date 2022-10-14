/**
 * Copyright (c) 2016, tencent, TDBank All Rights Reserved.
 */

package org.apache.inlong.agent.mysql.relayLog;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * ClassName:MemRelayLogImpl <br/>
 * Date:     2016年11月15日  下午3:28:15 <br/>
 * @author   lyndldeng
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class MemRelayLogImpl implements RelayLog {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final long MAX_DATA_SIZE;
    private LinkedList<byte[]> dataList;
    private AtomicLong containDataLength;
    private AtomicBoolean bRunning;
    private ReentrantLock reenLock;

    public MemRelayLogImpl(long maxDataSize){
        MAX_DATA_SIZE = maxDataSize;
        dataList = new LinkedList<byte[]>();
        bRunning = new AtomicBoolean(true);
        containDataLength = new AtomicLong(0l);
        reenLock = new ReentrantLock();
    }

    /**
     * @see RelayLog#putLog(byte[])
     */
    @Override
    public boolean putLog(byte[] bytes) {

        if(bytes == null || bytes.length <= 0){
            return true;
        }

        while((containDataLength.get() + bytes.length) > MAX_DATA_SIZE && bRunning.get()){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {}
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

    /**
     * @see RelayLog#getLog()
     */
    @Override
    public byte[] getLog() {

        byte[] bytes = null;
        try {
            reenLock.lock();
            bytes = dataList.poll();
        } finally {
            reenLock.unlock();
        }
        if(bytes != null){
            long delSize = bytes.length * -1;
            containDataLength.addAndGet(delSize);
        }

        return bytes;
    }

    /**
     * @see RelayLog#close()
     */
    @Override
    public void close() {
        bRunning.set(false);
    }

    /**
     * @see RelayLog#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * @see RelayLog#clearLog()
     */
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

    /**
     * @see RelayLog#report()
     */
    @Override
    public void report() {
        logger.debug("dataList size : " + dataList.size() + " , data size : " + containDataLength.get() / 1024 / 1024);
    }

}

