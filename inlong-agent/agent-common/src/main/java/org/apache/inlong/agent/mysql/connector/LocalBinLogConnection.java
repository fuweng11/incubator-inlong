package org.apache.inlong.agent.mysql.connector;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.mysql.connector.binlog.event.QueryLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.local.BinLogFileQueue;
import org.apache.inlong.agent.mysql.connector.binlog.LogContext;
import org.apache.inlong.agent.mysql.connector.binlog.LogDecoder;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.LogPosition;
import org.apache.inlong.agent.mysql.connector.dbsync.FileLogFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * local bin log connection (not real connection)
 * 
 * @author yuanzu Date: 12-9-27 Time: 下午6:14
 */
public class LocalBinLogConnection implements ErosaConnection {

    private static final Logger logger     = LogManager.getLogger(LocalBinLogConnection.class);
    private BinLogFileQueue binlogs    = null;
    private boolean             needWait;
    private String              directory;
    private int                 bufferSize = 16 * 1024;
    private boolean             running    = false;

    public LocalBinLogConnection(){
    }

    public LocalBinLogConnection(String directory, boolean needWait){
        this.needWait = needWait;
        this.directory = directory;
    }

    @Override
    public void connect() throws IOException {
        if (this.binlogs == null) {
            this.binlogs = new BinLogFileQueue(this.directory);
        }
        this.running = true;
    }

    @Override
    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() throws IOException {
        this.running = false;
        if (this.binlogs != null) {
            this.binlogs.destory();
        }
        this.binlogs = null;
        this.running = false;
    }

    public boolean isConnected() {
        return running;
    }

    @SuppressWarnings("rawtypes")
    public void seek(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        File current = new File(directory, binlogfilename);

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
            while (running) {
                boolean needContinue = true;
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    if (!func.sink(event)) {
                        needContinue = false;
                        break;
                    }
//                    do {
//                        event = decoder.decode(fetcher, context);
//                        if (event != null && !func.sink(event)) {
//                            needContinue = false;
//                            break;
//                        }
//                    } while (event != null);
                }

                fetcher.close(); // 关闭上一个文件
                if (needContinue) {// 读取下一个
                    File nextFile;
                    if (needWait) {
                        nextFile = binlogs.waitForNextFile(current);
                    } else {
                        nextFile = binlogs.getNextFile(current);
                    }

                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(nextFile.getName()));
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        } finally {
            fetcher.close();
        }
    }

    @SuppressWarnings("rawtypes")
    public void dump(long timestampMills, SinkFunction func) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current);            
            context.setLogPosition(new LogPosition(current.getName()));
            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = 4;
                long currentOffset = 0L;

                binlogFilename = lastXidLogFilename;
                binlogFileOffset = lastXidLogFileOffset;
                while (fetcher.fetch()) {
                    LogEvent event;
                    do {
                        event = decoder.decode(fetcher, context);
                        if (event != null && timestampSeconds > event.getWhen()) {
                            needContinue = false;
                            break;
                        }
                    } while (event != null);
                    
                    if(event == null){
                        break;
                    }

                    currentOffset += event.getEventLen();
                    if (LogEvent.QUERY_EVENT == event.getHeader().getType()) {
                        if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN")) {
                            binlogFilename = lastXidLogFilename;
                            binlogFileOffset = lastXidLogFileOffset;
                        } else if (LogEvent.XID_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = currentOffset;
                        }
                    }
                }

                if (needContinue) {// 读取下一个
                    fetcher.close(); // 关闭上一个文件

                    File nextFile = binlogs.getBefore(current);
                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(current.getName()));
                } else {
                    break;// 跳出
                }
            }
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }
        
        dump(binlogFilename, binlogFileOffset, func);
    }

    public ErosaConnection fork() {
        LocalBinLogConnection connection = new LocalBinLogConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);
        return connection;
    }

    public boolean isNeedWait() {
        return needWait;
    }

    public void setNeedWait(boolean needWait) {
        this.needWait = needWait;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

}
