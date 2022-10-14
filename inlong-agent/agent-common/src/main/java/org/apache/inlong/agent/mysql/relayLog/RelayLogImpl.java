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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;
import org.apache.inlong.agent.utils.DBSyncUtils;

public class RelayLogImpl implements RelayLog {

    private static final Logger logger = LogManager.getLogger(RelayLogImpl.class);
    private static final byte[] magicBytes = new byte[] { 0x01, 0x05, (byte) 0xBA, (byte) 0xDB };
    private final String relayPrefix;
    private final String logPath;

    private DBSyncConf config;

    private volatile long writePos = 0l;
    private AtomicInteger writeFileIndex = new AtomicInteger(0);
    private RandomAccessFile writeFile;
    private FileChannel writeFileChannel;
    private long addBufTime = 0l;

    private AtomicLong readPos = new AtomicLong(0);
    private AtomicInteger readFileIndex = new AtomicInteger(0);
    private RandomAccessFile readFile;
    private FileChannel readFileChannel;

    private final long fileSize;
    private final int blockSize;
    private final ByteBuffer readByeBufs;
    private final ByteBuffer writeByeBufs;

    private LinkedBlockingQueue<byte[]> queue;

    private final Lock fileLock;
    private final Lock buffLock;
    private boolean bQueueEmpty = true;
    private HashMap<Integer, Long> fileSizeMap;

    private long lastGetDataTime = Long.MAX_VALUE;
    private final int diffFileIndex;

    private AtomicBoolean bNeedReDump = new AtomicBoolean(false);

    public RelayLogImpl(String relayPrefix, String logPath, DBSyncConf dbSyncConf) {
        if (relayPrefix == null || logPath == null) {
            throw new IllegalArgumentException("log prefix & log root path can't be null");
        }

        this.config = dbSyncConf;

        this.logPath = logPath.endsWith("/") ? logPath : logPath + "/";
        checkDir(this.logPath);
        this.relayPrefix = relayPrefix;

        fileSize = config.getLongVar(ConfVars.RELAY_LOG_FILE_SIZE);
        blockSize = config.getIntVar(ConfVars.RELAY_LOG_BLOCK_SIZE);
        int tmpDiffFileIndex = config.getIntVar(ConfVars.RELAY_LOG_DIFF_FILE_INDEX);
        if (tmpDiffFileIndex <= 0) {
            diffFileIndex = 3;
        } else {
            diffFileIndex = tmpDiffFileIndex;
        }

        readByeBufs = ByteBuffer.allocate(blockSize);
        writeByeBufs = ByteBuffer.allocate(blockSize);

        queue = new LinkedBlockingQueue<byte[]>(1000);
        fileSizeMap = new HashMap<Integer, Long>();

        fileLock = new ReentrantLock();
        buffLock = new ReentrantLock();
    }

    @Override
    public boolean putLog(byte[] bytes) {
        boolean bAddQueue = false;
        /*
         * if (bQueueEmpty) { bAddQueue = queue.offer(bytes); if (!bAddQueue) {
         * bQueueEmpty = false; } }
         */

        if (!bAddQueue) {
            appendBytes(bytes);
        }

        if (bNeedReDump.get()) {
            clearLogImpl();
            return false;
        }

         while((writeFileIndex.get() - readFileIndex.get()) > diffFileIndex){
             try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
         }

        return true;
    }

    @Override
    public byte[] getLog() {
        byte[] data = queue.poll();
        if (data == null) {
            if (readFileIndex.get() < writeFileIndex.get() || readPos.get() < writePos || writeByeBufs.position() > 0) {
                // readChannel.re
                while (queue.remainingCapacity() > 0) {

                    if (!peekFileData()) {
                        long nowMillSec = System.currentTimeMillis();
                        if (writeByeBufs.position() > 0) {
                            if (nowMillSec - addBufTime >= 5000) {
                                flushWriteBuf();
                            }
                        }
                        break;
                    }
                }
                data = queue.poll();

                if (data != null) {
                    lastGetDataTime = System.currentTimeMillis();
                }

                return data;
            } else {
                if (readFileIndex.get() == writeFileIndex.get() && readPos.get() == writePos && writeByeBufs.position() == 0) {
                    try {
                        fileLock.lock();
                        buffLock.lock();
                        if (0 != writePos && readFileIndex.get() == writeFileIndex.get() && readPos.get() == writePos
                                && writeByeBufs.position() == 0 && !bQueueEmpty) {
                            try {
                                writeFileChannel.close();
                                writeFileChannel = null;
                                writeFile.close();
                                writeFile = null;

                                File tmpWriteFile = getFileByIndex(this.writeFileIndex.get());
                                if (tmpWriteFile.exists()) {
                                    tmpWriteFile.delete();
                                }
                                logger.error("need delete from in file index : " + this.readFileIndex.get());

                            } catch (IOException e) {
                                String fileName = String.format("%s%s-relay-%06d.log",
                                        new Object[] { logPath, relayPrefix, writeFileIndex.get() });
                                logger.error("close write file error, " + fileName + "\n"
                                        + DBSyncUtils.getExceptionStack(e));
                            }

                            readPos.set(0);
                            writePos = 0;
                            this.writeFileIndex.incrementAndGet();
                            this.readFileIndex.set(this.writeFileIndex.get());
                            bQueueEmpty = true;
                        }
                    } finally {
                        buffLock.unlock();
                        fileLock.unlock();
                    }
                }
            }
        }

        lastGetDataTime = System.currentTimeMillis();
        return data;
    }

    @Override
    public boolean isEmpty() {
        if (this.readFileIndex.get() == this.writeFileIndex.get() && this.readPos.get() == this.writePos && this.queue.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        FileUtils.deleteQuietly(new File(this.logPath));
        clearLogImpl();
    }

    public void clearLog() {
        clearLogImpl();
    }

    public void report(){

    }

    private void clearLogImpl() {
        try {
            fileLock.lock();
            buffLock.lock();
            closeWriteFile();
            for (int i = readFileIndex.get(); i <= writeFileIndex.get(); i++) {
                File tmpFile = getFileByIndex(i);
                if (tmpFile.exists()) {
                    tmpFile.delete();
                }
            }
            writeFileIndex.incrementAndGet();
            readFileIndex.set(writeFileIndex.get());
            readPos.set(0);
            writePos = 0;
            queue.clear();
            writeByeBufs.clear();
            bNeedReDump.set(false);
            bQueueEmpty = true;
        } finally {
            buffLock.unlock();
            fileLock.unlock();
        }
    }

    private void checkDir(final String path) {
        File dir = new File(path);
        FileUtils.deleteQuietly(dir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Create directory failed: " + dir.getAbsolutePath());
            }
        }
    }

    private File getFileByIndex(int fileIndex) {
        String readRelayFileName = this.logPath
                + String.format("%s-relay-%06d.log", new Object[] { relayPrefix, fileIndex });

        File tempFile = new File(readRelayFileName);
        return tempFile;
    }

    synchronized private void appendBytes(byte[] buf) {
        // when write buffer is full, flush to file
        long estimatedTime = System.currentTimeMillis() - lastGetDataTime;
        if (writeByeBufs.remaining() < (buf.length + 8) || estimatedTime > 5000) {
            flushWriteBuf();
        }

        try {
            buffLock.lock();
            writeByeBufs.put(magicBytes);
            int len = buf.length;
            byte[] lenByte = DBSyncUtils.intToBigEndian(len);
            writeByeBufs.put(lenByte);
            writeByeBufs.put(buf);
            addBufTime = System.currentTimeMillis();
        } finally {
            buffLock.unlock();
        }
    }

    synchronized private void flushWriteBuf() {

        writeByeBufs.flip();
        int writedSize = 0;
        int needWriteSize = writeByeBufs.remaining();
        boolean needLock = writeFileIndex.get() == readFileIndex.get();

        try {
            if (needLock) {
                fileLock.lock();
            }
            buffLock.lock();
            String fileName = String.format("%s%s-relay-%06d.log",
                    new Object[] { logPath, relayPrefix, writeFileIndex.get() });

            int retryCnt = 0;
            while (writeFile == null) {
                try {
                    initWriteFile();
                    logger.info("init write " + fileName + " success!");
                } catch (SecurityException se) {
                    logger.error("init write " + fileName + " cann't write \n" + DBSyncUtils.getExceptionStack(se));
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
                    //String fileName = String.format("%s%s-relay-%06d.log",
                    //		new Object[] { logPath, relayPrefix, writeFileIndex });

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
        } finally {
            buffLock.unlock();
            if (needLock) {
                fileLock.unlock();
            }
        }

        // this write file size is large than fileSize;
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
                logger.error("init write " + fileName + " cann't write \n" + DBSyncUtils.getExceptionStack(se));
                DBSyncUtils.sleep(1000);
            } catch (Exception e) {
                logger.error("init write file error, " + fileName + "\n" + DBSyncUtils.getExceptionStack(e));
            }
        }
    }

    private void initWriteFile() throws Exception {
        File tmpWriteFile = getFileByIndex(this.writeFileIndex.get());
        if (tmpWriteFile.exists()) {
            tmpWriteFile.delete();
        }
        writeFile = new RandomAccessFile(tmpWriteFile, "rw");
        writeFileChannel = writeFile.getChannel();
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
                logger.error("init read " + fileName + " cann't read&write \n" + DBSyncUtils.getExceptionStack(se));
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
                ret = readFileChannel.read(readByeBufs, readPos.get());
                //logger.debug("Get data from {} , from position {} , data length {} ",
                //		new Object[] { fileName, readPos, ret });
                readByeBufs.flip();
                break;
            } catch(ClosedChannelException ce){
                readByeBufs.clear();
                logger.error("read data from " + fileName + " , position : " + readPos.get() + " occure closed Exception\n"
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
                logger.error("read data from " + fileName + " , position : " + readPos.get() + "\n"
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
                    queue.clear();
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
                    if (!queue.offer(body)) {
                        return false;
                    }
                    readPos.set(readPos.get() + len + 8);
                } else {
                    if (ret == -1) {
                        bHasData = false;
                    }
                    break;
                }
            } else {
                if (ret == -1) {
//					String fileName = String.format("%s%s-relay-%06d.log", 
//							new Object[] { logPath, relayPrefix, readFileIndex });
//					File readFile = new File(fileName);
//					long readingFilePos = this.writePos;
//					if(readFileIndex < writeFileIndex){
//						readingFilePos = fileSizeMap.get(readFileIndex);
//					}
                    if (((readFileIndex.get() < writeFileIndex.get())
                            && (readPos.get() == fileSizeMap.get(readFileIndex.get())))) {
                        removeReadFile();
                        fileSizeMap.remove(readFileIndex.get());
                        readFileIndex.incrementAndGet();
                        readPos.set(0);
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
}
