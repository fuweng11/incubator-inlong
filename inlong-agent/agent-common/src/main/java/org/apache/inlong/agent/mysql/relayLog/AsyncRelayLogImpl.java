package org.apache.inlong.agent.mysql.relayLog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.utils.DBSyncUtils;

public class AsyncRelayLogImpl extends AbstractRelayLog {
    private static final Logger logger = LogManager.getLogger(AsyncRelayLogImpl.class);
    private LinkedBlockingQueue<byte[]> queue;
    private HashMap<Integer, Long> fileSizeMap;

    private final ByteBuffer readByeBufs;
    private final ByteBuffer writeByeBufs;

    private final Lock fileLock;

    private long writePos = 0l;
    private AtomicInteger writeFileIndex = new AtomicInteger(0);
    private RandomAccessFile writeFile;
    private FileChannel writeFileChannel;
    private long addBufTime = 0l;

    private LinkedBlockingQueue<byte[]> rqueue;

    private long readPos = 0l;
    private AtomicInteger readFileIndex = new AtomicInteger(0);
    private RandomAccessFile readFile;
    private FileChannel readFileChannel;
    private AtomicBoolean bNeedReDump = new AtomicBoolean(false);

    private WriteThread writer;

    public AsyncRelayLogImpl(String relayPrefix, String logPath, DBSyncConf dbSyncConf){
        super(relayPrefix, logPath, dbSyncConf);
        queue = new LinkedBlockingQueue<byte[]>(1000);
        fileSizeMap = new HashMap<Integer, Long>();

        readByeBufs = ByteBuffer.allocate(blockSize);
        writeByeBufs = ByteBuffer.allocate(blockSize);
        fileLock = new ReentrantLock();
        rqueue = new LinkedBlockingQueue<byte[]>();

        writer = new  WriteThread();
        writer.setName(relayPrefix + "-writer");
        writer.start();
    }

    @Override
    public boolean putLog(byte[] bytes) {

        if (bNeedReDump.get()) {
            clearLogImpl();
            return false;
        }

        do {
            try {
                if (queue.offer(bytes, 1, TimeUnit.MICROSECONDS)) {
                    break;
                }
            } catch (Throwable e) {
                logger.error("append data to queue error!\n"
                        + DBSyncUtils.getExceptionStack(e));
            }
        } while(true);

        return true;
    }

    @Override
    public byte[] getLog() {

        if(rqueue.isEmpty()){
            //get data from file
            peekFileData();
        }

        byte[] byteData = rqueue.poll();

        return byteData;
    }

    @Override
    public void close() {
        FileUtils.deleteQuietly(new File(this.logPath));
        clearLogImpl();
    }

    @Override
    public boolean isEmpty() {
        if (this.readFileIndex.get() == this.writeFileIndex.get()
                && this.readPos == this.writePos && this.rqueue.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public void clearLog() {
        clearLogImpl();
    }

    public void report(){
        logger.debug("asyc-relay-log input queue size : {}, output queue size : {}", new Object[]{queue.size(), rqueue.size()});
    }

    private void clearLogImpl() {
        try {
            fileLock.lock();
            closeWriteFile();
            for (int i = readFileIndex.get(); i <= writeFileIndex.get(); i++) {
                File tmpFile = getFileByIndex(i);
                if (tmpFile.exists()) {
                    tmpFile.delete();
                }
            }
            writeFileIndex.incrementAndGet();
            readFileIndex.set(writeFileIndex.get());
            readPos = 0;
            writePos = 0;
            queue.clear();
            rqueue.clear();
            writeByeBufs.clear();
            bNeedReDump.set(false);
        } finally {
            fileLock.unlock();
        }
    }

    private void closeWriteFile() {
        try {
            if (writeFileChannel != null) {
                writeFileChannel.close();
            }

            if (writeFile != null) {
                writeFile.close();
            }
        } catch (IOException e) {
            String fileName = String.format("%s%s-relay-%06d.log",
                    new Object[] { logPath, relayPrefix, writeFileIndex.get() });
            logger.error("close file error, " + fileName + " maybe not close!\n" + DBSyncUtils.getExceptionStack(e));
        }
        writeFileChannel = null;
        writeFile = null;
    }

    private File getFileByIndex(int fileIndex) {
        String readRelayFileName = this.logPath
                + String.format("%s-relay-%06d.log", new Object[] { relayPrefix, fileIndex });

        File tempFile = new File(readRelayFileName);
        return tempFile;
    }

    private void initWriteFile() throws Exception {
        File tmpWriteFile = getFileByIndex(this.writeFileIndex.get());
        if (tmpWriteFile.exists()) {
            tmpWriteFile.delete();
        }
        writeFile = new RandomAccessFile(tmpWriteFile, "rw");
        writeFileChannel = writeFile.getChannel();
    }

    private void flushBufferToFile(long timeStample){
        writeByeBufs.flip();
        int writedSize = 0;
        int needWriteSize = writeByeBufs.remaining();

        boolean needLock = writeFileIndex.get() == readFileIndex.get();
        addBufTime = timeStample;

        try{
            if(needLock){
                fileLock.lock();
            }
            String fileName = String.format("%s%s-relay-%06d.log",
                    new Object[] { logPath, relayPrefix, writeFileIndex.get() });

            int retryCnt = 0;

            while (writeFile == null) {
                try {
                    initWriteFile();
                    logger.info("init write " + fileName + " success!");
                } catch (SecurityException se) {
                    logger.error("init write " + fileName + " can't write \n" + DBSyncUtils.getExceptionStack(se));
                    DBSyncUtils.sleep(1000);
                } catch (Exception e) {
                    if (retryCnt >= 10) {
                        bNeedReDump.set(true);
                        break;
                    }
                    retryCnt++;
                    //String fileName = String.format("%s%s-relay-%06d.log",
                    //		new Object[] { logPath, relayPrefix, writeFileIndex });
                    logger.error("init write file error, " + fileName + "\n" + DBSyncUtils.getExceptionStack(e));
                    DBSyncUtils.sleep(1);
                }
            }

            //check file is delete
            File tmpWriteFile = new File(fileName);
            if(!tmpWriteFile.exists() || tmpWriteFile.length() != writePos){
                //need read dump
                bNeedReDump.set(true);
                logger.error("write file {} de delete, need re dump binlog!", fileName);
            }

            long startWritePos = writePos;
            int bufWritePos = writeByeBufs.position();
            retryCnt = 0;
            do {
                try {
                    int ret = writeFileChannel.write(writeByeBufs, startWritePos);
                    writedSize = writedSize + ret;
                    startWritePos = startWritePos + ret;
                } catch (IOException e) {
                    if (retryCnt >= 10) {
                        bNeedReDump.set(true);
                        break;
                    }
                    retryCnt++;
                    startWritePos = writePos;
                    writeByeBufs.position(bufWritePos);

                    //check space
                    try {
                        long freeSpaceLength = 0;
                        do {
                            long freeSpaceKb = FileSystemUtils.freeSpaceKb(logPath, 1000 * 60);
                            freeSpaceLength = freeSpaceKb * 1024;

                            if(freeSpaceLength < needWriteSize){
                                logger.error("{} not has space for write {} Bytes Buffer!", new Object[]{logPath, needWriteSize});
                                DBSyncUtils.sleep(1000 * 10);
                            }
                        } while(freeSpaceLength < needWriteSize);
                    } catch (IOException sie) {
                        logger.error("check {} space error : {}", new Object[]{logPath, DBSyncUtils.getExceptionStack(sie)});
                    }

                    logger.error(
                            "close file error, " + fileName + " maybe not close!\n" + DBSyncUtils.getExceptionStack(e));
                }
            } while (writedSize < needWriteSize);
            writeByeBufs.clear();
            writePos += needWriteSize;
        } finally{
            if (needLock) {
                fileLock.unlock();
            }
        }

        // get a new file begin write
        if (writePos >= fileSize) {
            closeWriteFile();
            fileSizeMap.put(writeFileIndex.get(), writePos);
            writeFileIndex.incrementAndGet();
            writePos = 0l;
            String fileName = String.format("%s%s-relay-%06d.log",
                    new Object[] { logPath, relayPrefix, writeFileIndex.get() });
            try {
                initWriteFile();
            } catch (SecurityException se) {
                logger.error("init write " + fileName + " can't write \n" + DBSyncUtils.getExceptionStack(se));
                DBSyncUtils.sleep(1000);
            } catch (Exception e) {
                logger.error("init write file error, " + fileName + "\n" + DBSyncUtils.getExceptionStack(e));
            }
        }

    }

    private int readDataToBuffer() {

        int retryCnt = 0;
        String fileName = String.format("%s%s-relay-%06d.log", new Object[] { logPath, relayPrefix, readFileIndex.get() });
        while (readFile == null) {
            try {
                File tmpReadFile = getFileByIndex(this.readFileIndex.get());

                if(this.readFileIndex.get() < this.writeFileIndex.get() && !tmpReadFile.exists()){
                    bNeedReDump.set(true);
                    logger.error("read file {} de delete, need re dump binlog!", fileName);
                    return -2;
                }

                readFile = new RandomAccessFile(tmpReadFile, "r");
            } catch (FileNotFoundException e) {

                if(this.readFileIndex.get() == this.writeFileIndex.get()){
                    return -2;
                }

                if (retryCnt >= 10) {
                    bNeedReDump.set(true);
                    break;
                }
                retryCnt++;
                logger.error("init read file error, " + fileName + "\n" + DBSyncUtils.getExceptionStack(e));
            } catch(SecurityException se){
                logger.error("init read " + fileName + " can't read&write \n" + DBSyncUtils.getExceptionStack(se));
                if (retryCnt >= 10) {
                    return -2;
                }
                DBSyncUtils.sleep(1000);
                retryCnt++;
            }
        }

        int ret = -2;
        retryCnt = 0;
        do {
            try {
                fileLock.lock();
                readByeBufs.clear();
                readFileChannel = readFile.getChannel();
                ret = readFileChannel.read(readByeBufs, readPos);
                //logger.debug("Get data from {} , from position {} , data length {} ",
                //		new Object[] { fileName, readPos, ret });
                readByeBufs.flip();
                break;
            } catch(ClosedChannelException ce){
                readByeBufs.clear();
                logger.error("read data from " + fileName + " , position : " + readPos + " occure closed Exception\n"
                        + DBSyncUtils.getExceptionStack(ce));
                break;
            } catch (IOException e) {
                if (retryCnt >= 10) {
                    bNeedReDump.set(true);
                    break;
                }
                retryCnt++;
                // String fileName = String.format("%s%s-relay-%06d.log",
                // new Object[] { logPath, relayPrefix, readFileIndex});
                logger.error("read data from " + fileName + " , position : " + readPos + "\n"
                        + DBSyncUtils.getExceptionStack(e));
            } finally {
                fileLock.unlock();
            }
        } while (true);

        try {
            readFileChannel.close();
            readFile.close();
        } catch (IOException e) {
            // String fileName = String.format("%s%s-relay-%06d.log",
            // new Object[] { logPath, relayPrefix, readFileIndex});
            logger.error("close " + fileName + " error\n" + DBSyncUtils.getExceptionStack(e));
        }
        readFileChannel = null;
        readFile = null;

        return ret;
    }

    private boolean peekFileData() {
        boolean bHasData = true;
        int ret = 0;
        if (!bNeedReDump.get()) {
            ret = readDataToBuffer();
            if(ret == -2){
                return false;
            }
        } else {
            return false;
        }
        while (true) {
            if ((readByeBufs.limit() - readByeBufs.position()) >= 8) {
                byte[] head = new byte[4];
                readByeBufs.get(head, 0, 4);

                if (!checkHead(head)) {
                    bNeedReDump.set(true);
                    // clear all the not parse data;
                    rqueue.clear();
                    String errorMagic = DBSyncUtils.byteArrayToString(head);
                    logger.error("head magic error, need re dump binlog, error magic head is " + errorMagic);
                    return false;
                }

                byte[] lenBytes = new byte[4];
                readByeBufs.get(lenBytes, 0, 4);

                int len = DBSyncUtils.bigEndianToInt(lenBytes);

                if ((readByeBufs.limit() - readByeBufs.position()) >= len) {
                    byte[] body = new byte[len];
                    readByeBufs.get(body, 0, len);
                    if (!rqueue.offer(body)) {
                        return false;
                    }
                    readPos = readPos + len + 8;
                } else {
                    if (ret == -1) {
                        bHasData = false;
                    }
                    break;
                }
            } else {
                if (ret == -1) {
                    if (((readFileIndex.get() < writeFileIndex.get()) && (readPos == fileSizeMap.get(readFileIndex.get())))) {
                        removeReadFile();
                        fileSizeMap.remove(readFileIndex.get());
                        readFileIndex.incrementAndGet();
                        readPos = 0;
                    }else {
                        bHasData = false;
                    }
                }
                break;
            }
        }

        return bHasData;
    }

    private void removeReadFile() {

        String fileName = String.format("%s%s-relay-%06d.log", new Object[] { logPath, relayPrefix, readFileIndex.get() });

        File readedFile = getFileByIndex(this.readFileIndex.get());
        if (readedFile.exists()) {
            readedFile.delete();
            logger.info("delete readed file " + fileName);
        }
    }

    private boolean checkHead(byte[] head) {
        if (head.length != 4) {
            return false;
        }
        if (((head[0] ^ magicBytes[0]) | (head[1] ^ magicBytes[1]) | (head[2] ^ magicBytes[2])
                | (head[3] ^ magicBytes[3])) == 0x00) {
            return true;
        }
        return false;
    }

    private class WriteThread extends Thread{

        @Override
        public void run(){
            logger.info("AsyncRelayLogImpl WriteThread begin running!");
            long appendBufferTime = 0l;
            while (true) {
                try {
                    byte[] byteData = null;
                    try {
                        appendBufferTime = System.currentTimeMillis();
                        byteData = queue.poll(1, TimeUnit.MICROSECONDS);
                    } catch (InterruptedException e) {}

                    if(bNeedReDump.get()){
                        DBSyncUtils.sleep(1);
                    }

                    if(byteData == null){
                        if((appendBufferTime - addBufTime) > 5000 &&
                            writeByeBufs.position() > 0){
                            flushBufferToFile(appendBufferTime);
                        }
                        continue;
                    }

                    if(writeByeBufs.remaining() <= (byteData.length + 8)){
                        //flush byte data to file
                        flushBufferToFile(appendBufferTime);
                    }
                    writeByeBufs.put(magicBytes);
                    int len = byteData.length;
                    byte[] lenByte = DBSyncUtils.intToBigEndian(len);
                    writeByeBufs.put(lenByte);
                    writeByeBufs.put(byteData);
                    if(writeByeBufs.remaining() < 100){
                        //flush byte data to file
                        flushBufferToFile(appendBufferTime);
                    }
                } catch (Throwable t) {
                    logger.error("WriteThread catch unknown error!\n"
                            + DBSyncUtils.getExceptionStack(t));
                }
            }
        }
    }

}
