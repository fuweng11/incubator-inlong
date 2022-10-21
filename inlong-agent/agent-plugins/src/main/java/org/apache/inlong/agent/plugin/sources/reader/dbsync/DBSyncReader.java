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

package org.apache.inlong.agent.plugin.sources.reader.dbsync;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.common.protocol.CanalEntry.EntryType;
import org.apache.inlong.agent.common.protocol.CanalEntry.RowChange;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.Column;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.RowData;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.except.DBSyncServerException.JobInResetStatus;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.mysql.connector.AuthenticationInfo;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.MysqlConnection.BinlogFormat;
import org.apache.inlong.agent.mysql.connector.MysqlConnection.BinlogImage;
import org.apache.inlong.agent.mysql.connector.SinkFunction;
import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogContext;
import org.apache.inlong.agent.mysql.connector.binlog.LogDecoder;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.FormatDescriptionLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.LogHeader;
import org.apache.inlong.agent.mysql.connector.binlog.event.RotateLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.LogEventConvert;
import org.apache.inlong.agent.mysql.connector.dbsync.TableMetaCache;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.connector.exception.BinlogMissException;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.connector.exception.TableIdNotFoundException;
import org.apache.inlong.agent.mysql.filter.CanalEventFilter;
import org.apache.inlong.agent.mysql.parse.BinlogParser;
import org.apache.inlong.agent.mysql.protocol.position.EntryPosition;
import org.apache.inlong.agent.mysql.protocol.position.LogIdentity;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.mysql.relaylog.AsyncRelayLogImpl;
import org.apache.inlong.agent.mysql.relaylog.MemRelayLogImpl;
import org.apache.inlong.agent.mysql.relaylog.RelayLog;
import org.apache.inlong.agent.mysql.relaylog.exception.RelayLogPosErrorException;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.reader.AbstractReader;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.State;
import org.apache.inlong.agent.state.JobStat.TaskStat;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.ErrorCode;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.MonitorLogUtils;
import org.apache.inlong.agent.utils.countmap.UpdaterMap;
import org.apache.inlong.agent.utils.countmap.UpdaterMapFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_ACK_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_BINLOG_START_OFFEST;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_IS_DEBUG_MODE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_IS_NEED_TRANSACTION;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_JOB_DO_SWITCH_CNT;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_JOB_RECV_BUFFER_KB;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_MEM_SZIE;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_ROOT;
import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_RELAY_LOG_WAY;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_ACK_THREAD_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_IS_DEBUG_MODE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_JOB_DO_SWITCH_CNT;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_JOB_RECV_BUFFER_KB;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_RELAY_LOG_ROOT;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_IS_NEED_TRANSACTION;

/**
 * Refactor from AsyncParseMysqlJob in DBSync
 */
public class DBSyncReader extends AbstractReader {

    protected static final long SECOND_MV_BIT = 1000000000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DBSyncReader.class);
    private static final AtomicReferenceFieldUpdater<DBSyncReader, State> STATUS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DBSyncReader.class, JobStat.State.class, "status");
    private static final long MAX_SECOND = 9999999999L;
    private static AtomicLong randomIndex = new AtomicLong(0);
    private final AgentConfiguration agentConf;
    private final Object syncObject = new Object();
    private final LinkedBlockingQueue<DBSyncMessage> messageQueue;
    public volatile long pkgIndexId = 0L;
    protected DBSyncJobConf jobconf;
    protected int debugCnt = 0;
    protected AtomicBoolean needTransactionPosition = new AtomicBoolean(false);
    protected volatile boolean dispatchRunning = false;
    protected PkgEvent lastPkgEvent;
    protected ArrayList<Entry> lastEntryList;
    protected Entry lastEntry;
    protected LogPosition lastParsePos;
    protected UpdaterMap<Integer, AtomicLong> sendMsgCnt;
    protected AuthenticationInfo masterInfo;
    protected MysqlConnection metaConnection;
    protected EntryPosition masterPosition;
    protected boolean bBinlogMiss = false;
    protected int fallbackIntervalInSeconds = 10;
    protected CanalEventFilter<String> eventFilter;
    protected BinlogParser binlogParser;
    protected AtomicLong waitAckCnt;
    protected volatile boolean running = false;
    protected volatile long heartBeat = 0;
    private boolean isDebug;
    private String mysqlAddress;
    private String remoteIp;
    private int mysqlPort;
    private String userName;
    private String passwd;
    private String jobName;
    private LogPosition lastLog;
    private int reConnectCnt = 0;
    private int doSwitchCnt = 0;
    private RelayLog relayLog;
    private LogPosition parseLastLog;
    private LogPosition minEventLogPosition;
    private JobStat.State parseDisptcherStatus;
    private JSONObject stopHb = null;
    private TableMetaCache tableMetaCache;
    private AtomicBoolean needReset = new AtomicBoolean(false);
    private CompletableFuture<Void> resetFuture = null;
    private AtomicBoolean isSwitching = new AtomicBoolean(false);
    private AtomicBoolean inSwitchParseDone = new AtomicBoolean(false);
    private ArrayList<LogEvent> bufferList = new ArrayList<>();
    private ConcurrentHashMap<Integer, ParseThread> parseThreadList;
    private Thread dispatcherThread = null;
    private Thread dumpThread = null;
    private String currentDbInfo = "";
    private Semaphore semaphore;
    private Long lastTimeStamp;
    private long slaveId = -1;
    private long printStatIntervalMs = 60 * 1000 * 2;
    private LinkedBlockingQueue<LogPosition> ackLogPositionList = new LinkedBlockingQueue();
    private List<Thread> handleAckLogPositionThreadList = new LinkedList<>();
    private ConcurrentSkipListSet<LogPosition> sendLogPositionCache = new ConcurrentSkipListSet();
    private ConcurrentSkipListSet<LogPosition> eventLogPositionCache = new ConcurrentSkipListSet();
    private volatile JobStat.State status;
    private String errorMsg = "";
    private long oldSecStamp;
    private volatile long oldTimeStampler = 0;
    private static final String KEY_MYSQL_ADDRESS = "mysqlAddress";
    private static final String KEY_DBSYNC_JOB_NAME = "dbsyncJobName";
    private static final String KEY_SERVER_ID = "serverId";
    private static final String KEY_MYSQL_USER_NAME = "mysqlUserName";
    private static final String KEY_BINLOG_FILE_PATH = "binlogFilePath";
    private static final String KEY_BINLOG_START_POSITION = "binlogStartPosition";
    private static final String KEY_LAST_TIMESTAMP = "lastTimeStamp";

    public DBSyncReader(JobProfile profile) {
        // agent conf
        agentConf = AgentConfiguration.getAgentConf();
        isDebug = agentConf.getBoolean(DBSYNC_IS_DEBUG_MODE, DEFAULT_DBSYNC_IS_DEBUG_MODE);
        if (isDebug) {
            sendMsgCnt = UpdaterMapFactory.getIntegerUpdater();
        }
        if (agentConf.getBoolean(DBSYNC_IS_NEED_TRANSACTION, DEFAULT_IS_NEED_TRANSACTION)) {
            needTransactionPosition.compareAndSet(false, true);
            LOGGER.info("set need transaction position to true");
        }
        doSwitchCnt = agentConf.getInt(DBSYNC_JOB_DO_SWITCH_CNT, DEFAULT_DBSYNC_JOB_DO_SWITCH_CNT);
        messageQueue = new LinkedBlockingQueue<>(5000);//TODO:configurable in agent.properties
        // job conf
        jobconf = profile.getDbSyncJobConf();
        jobName = jobconf.getJobName();
        lastLog = jobconf.getStartPos();
        int maxUnackedLogPositionsUnackedMessages = jobconf.getMaxUnackedLogPositions();
        semaphore = new Semaphore(maxUnackedLogPositionsUnackedMessages);
        mysqlAddress = jobconf.getCurMysqlIp();
        mysqlPort = jobconf.getCurMysqlPort();
        userName = jobconf.getMysqlUserName();
        passwd = jobconf.getMysqlPassWd();
        masterInfo = new AuthenticationInfo(new InetSocketAddress(mysqlAddress, mysqlPort), userName, passwd);
        eventFilter = jobconf.getFilter();
        // prepare work
        binlogParser = buildParser();
        waitAckCnt = new AtomicLong(0); //TODO:update
        relayLog = mkRelayLog();
        slaveId = generateSlaveId();
        parseThreadList = new ConcurrentHashMap<>();
        lastTimeStamp = Instant.now().toEpochMilli();
        Map<String, String> readerMetricDimensions = readerMetric.getDimensions();
        readerMetricDimensions.put(KEY_MYSQL_ADDRESS, mysqlAddress);
        readerMetricDimensions.put(KEY_DBSYNC_JOB_NAME, jobName);
        readerMetricDimensions.put(KEY_SERVER_ID, jobconf.getServerId());
        readerMetricDimensions.put(KEY_MYSQL_USER_NAME, jobconf.getMysqlUserName());
        readerMetricDimensions.put(KEY_BINLOG_FILE_PATH, jobconf.getBinlogFilePath());
        readerMetricDimensions.put(KEY_BINLOG_START_POSITION, String.valueOf(lastLog));
        readerMetricDimensions.put(KEY_LAST_TIMESTAMP, String.valueOf(lastTimeStamp));
    }

    public DBSyncJobConf getJobconf() {
        return jobconf;
    }

    @Override
    public void init(JobProfile jobConf) {
        super.init(jobConf);
        start();
    }

    private void start() {
        running = true;
        setState(State.INIT); //TODO: move to abstractReader init?
        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_INIT);
        int ackThreadNum = agentConf.getInt(DBSYNC_ACK_THREAD_NUM, DEFAULT_ACK_THREAD_NUM);
        for (int i = 0; i < ackThreadNum; i++) {
            AckLogPositionThread thread = new AckLogPositionThread();
            thread.setName("AckLogPosition-" + i + "-" + this.jobName);
            thread.start();
            handleAckLogPositionThreadList.add(thread);
        }
        dispatchRunning = true;
        dispatcherThread = mkDispatcher();
        dumpThread = mkDumpThread();
        dumpThread.start();
        dispatcherThread.start();

        //TODO:add to monitor
//        monitor.initMonitor(this.jobName, System.currentTimeMillis(), this);

        setState(State.RUN);
        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_START_RUN);
    }

    @Override
    public Message read() {
        if (!messageQueue.isEmpty()) {
            return messageQueue.poll();
        }
        return null;
    }

    public void addMessage(DBSyncMessage message) {
        try {
            messageQueue.put(message);
            waitAckCnt.incrementAndGet();
        } catch (InterruptedException e) {
            LOGGER.error("put message to dbsyncReader queue error", e);
        }
    }

    @Override
    public boolean isFinished() {
        return !running;
    }

    //TODO: need?
    @Override
    public String getReadSource() {
        return null;
    }

    @Override
    public void setReadTimeout(long mill) {

    }

    @Override
    public void setWaitMillisecond(long millis) {

    }

    @Override
    public String getSnapshot() {
        return null;
    }

    @Override
    public void finishRead() {
        stop();
    }

    //TODO
    @Override
    public boolean isSourceExist() {
        return true;
    }

    @Override
    public State getState() {
        return null;
    }

    @Override
    public void setState(State state) {
        STATUS_UPDATER.set(DBSyncReader.this, state);
    }

    //TODO: is ok?
    @Override
    public void destroy() {
        finishRead();
    }

    public JobStat.State getJobStat() {
        return status;
    }

    public void setJobStat(JobStat.State state) {
        STATUS_UPDATER.set(DBSyncReader.this, state);
    }

    //TODO:move to job level?
    public synchronized void restart() {
        if (getJobStat() == JobStat.State.RUN) {
            return;
        }
        relayLog = mkRelayLog();
        try {
            start();
        } catch (Exception e) {
            LOGGER.error("Restart job {} has exception: ", jobName, e);
        }
    }

    public MysqlConnection buildErosaConnection(String mysqlAddress, int mysqlPort) {
        String ip = mysqlAddress;
        try {
            InetAddress[] addresses = InetAddress.getAllByName(mysqlAddress);
            if (addresses != null && addresses.length > 0) {
                int index = ((int) Instant.now().toEpochMilli()) % addresses.length;
                ip = addresses[index].getHostAddress();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        LOGGER.info("jobName {} buildErosaConnection url {}, ip = {}", jobName, mysqlAddress, ip);
        MysqlConnection connection = new MysqlConnection(new InetSocketAddress(ip, mysqlPort),
                jobconf.getMysqlUserName(), jobconf.getMysqlPassWd(), (byte) 33, "information_schema");
        int recvBufferSizeKb = agentConf.getInt(DBSYNC_JOB_RECV_BUFFER_KB, DEFAULT_DBSYNC_JOB_RECV_BUFFER_KB);
        connection.getConnector().setReceiveBufferSize(recvBufferSizeKb * 1024);
        connection.getConnector().setSendBufferSize(8 * 1024);
        connection.getConnector().setSoTimeout(30 * 1000);
        connection.setCharset(StandardCharsets.UTF_8);
        connection.setSlaveId(slaveId);

        return connection;
    }

    public synchronized void stop() {
        running = false;
        //TODO:stop from monitor
//        JSONObject lastHbInfo = .getLastHbInfo(this.jobName);
//        if (lastHbInfo != null) {
//            monitor.putStopHeartbeat(lastHbInfo);
//        }
//        monitor.removeJobMonitor(this.jobName);

        // stop acklog
        for (Thread thread : handleAckLogPositionThreadList) {
            if (thread != null) {
                thread.interrupt();
            }
        }
        // stop dispatch
        dispatchRunning = false;
        if (dispatcherThread != null) {
            dispatcherThread.interrupt();
            dispatcherThread = null;
        }
        // stop dump
        relayLog.close();
        if (dumpThread != null) {
            dumpThread.interrupt();
            dumpThread = null;
        }
        // stop parse
        if (parseDisptcherStatus != null && parseDisptcherStatus == JobStat.State.STOP) {
            if (parseThreadList != null) {
                for (ParseThread parser : parseThreadList.values()) {
                    parser.stopParse();
                    parser.interrupt();
                }
                parseThreadList = null;
            }
        }
    }

    private long generateSlaveId() {
        long result = 0;
        if (StringUtils.isNotEmpty(jobconf.getServerId())) {
            try {
                result += Long.parseLong(jobconf.getServerId());
                result += randomIndex.addAndGet(1);
            } catch (Exception e) {
                LOGGER.warn("server Id: {} invalid, {}, slave_id will use a random number.",
                        jobconf.getServerId(), e.getMessage());
                Random r = new Random();
                result += r.nextInt(1024);
            }
        } else {
            Random r = new Random();
            result += r.nextInt(1024);
        }
        LOGGER.info("{} create mysql slave_id is {}", jobName, slaveId);
        return result;
    }

    //TODO: jobMonitor use
    public long getHeartBeat() {
        return heartBeat;
    }

    public void setHeartBeat(long heartBeat) {
        this.heartBeat = heartBeat;
    }

    protected BinlogParser<LogEvent> buildParser() {
        LogEventConvert convert = new LogEventConvert();
        if (eventFilter != null && eventFilter instanceof CanalEventFilter<?>) {
            LOGGER.info("Job {} set job filter!", this.jobName);
            convert.setNameFilter(eventFilter);
        }

        convert.setCharset(jobconf.getCharset());
        return convert;
    }

    public LogPosition getLogPosition() {

        if (isDebug) {
            if (debugCnt > 120) {
                debugCnt = 0;
                for (String key : sendMsgCnt.keySet()) {
                    long cnt = sendMsgCnt.remove(key).get();
                    LOGGER.info(key + "#" + cnt);
                }
            } else {
                debugCnt++;
            }
        }
        LogPosition tmpParsePos = null;
        relayLog.report();

        try {
            if (parseThreadList != null) {
                for (ParseThread parser : parseThreadList.values()) {
                    LogPosition parsePos = parser.getLastParsPosition();

                    if (parsePos == null) {
                        continue;
                    }

                    if (tmpParsePos == null || parsePos.compareTo(tmpParsePos) > 0) {
                        tmpParsePos = parsePos;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("{} get job parse log position occur Exception : {}",
                    jobName, DBSyncUtils.getExceptionStack(e));
        }

        if (parseLastLog != null && tmpParsePos == null) {
            tmpParsePos = parseLastLog;
        }

        if (tmpParsePos != null) {
            return new LogPosition(tmpParsePos);
        }
        return null;
    }

    public LogPosition getMaxLogPosition() {
        LogPosition logPosition = null;
        if (metaConnection != null) {
            MysqlConnection maxCon = (MysqlConnection) metaConnection;
            try {
                EntryPosition maxEntryPos = findEndPosition(maxCon);
                logPosition = new LogPosition();
                logPosition.setPosition(maxEntryPos);

                LogIdentity identity = new LogIdentity(maxCon.getConnector().getAddress(), -1L);
                logPosition.setIdentity(identity);
            } catch (Throwable t) {
                LOGGER.warn("can't get max position " + ExceptionUtils.getStackTrace(t));
            } finally {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("GetMaxLogPosition finished!");
                }
            }
        }
        return logPosition;
    }

    private EntryPosition findEndPosition(MysqlConnection mysqlConnection) {
        try {
            if (!mysqlConnection.isConnected()) {
                mysqlConnection.connect();
            }
            ResultSetPacket packet;
            synchronized (mysqlConnection) {
                packet = mysqlConnection.query("show master status");
            }
            List<String> fields = packet.getFieldValues();
            if (DBSyncUtils.isCollectionsEmpty(fields)) {
                throw new CanalParseException(
                        "command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,"
                                + "REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.parseLong(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException(
                    "command : 'show master status' has an error!" + mysqlConnection.getConnector().getAddress()
                            .toString(), e);
        }
    }

    public LogPosition getMaxParseLogPosition() {
        LogPosition tmpParsePos = null;
        try {
            if (parseThreadList != null) {
                for (ParseThread parser : parseThreadList.values()) {
                    LogPosition parsePos = parser.getLastParsPosition();

                    if (parsePos == null) {
                        continue;
                    }

                    if (tmpParsePos == null
                            || (parsePos.getPosition().getTimestamp() > tmpParsePos.getPosition().getTimestamp()
                            || parsePos.getPosition().getPosition() > tmpParsePos.getPosition().getPosition())) {
                        tmpParsePos = parsePos;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("{} get job parse log position occur Exception : {}",
                    jobName, DBSyncUtils.getExceptionStack(e));
        }
        return tmpParsePos;
    }

    @Override
    public long ackJobData(int cnt) {
        long ackResult = waitAckCnt.addAndGet(-cnt);
        if (ackResult < 0) {
            LOGGER.debug("ackCnt is: {}, ackResult is: {}", cnt, ackResult);
        }
        return ackResult;
    }

    //TODO: use, updateConnectionCharset
    public void updateConnectionCharset() {
        for (ParseThread parser : parseThreadList.values()) {
            parser.updateCharSet(jobconf.getCharset());
        }
    }

    public LogPosition getMaxProcessedPosition() {
        LogPosition tmpProcessedPos = null;
        try {
            if (parseThreadList != null) {
                for (ParseThread parser : parseThreadList.values()) {
                    LogPosition parsePos = parser.getLastProcessedPosition();
                    if (parsePos == null) {
                        continue;
                    }
                    if (tmpProcessedPos == null || parsePos.compareTo(tmpProcessedPos) > 0) {
                        tmpProcessedPos = parsePos;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("{} get job parse log position occur Exception : {}",
                    jobName, DBSyncUtils.getExceptionStack(e));
        }
        if (tmpProcessedPos != null) {
            return new LogPosition(tmpProcessedPos);
        }
        return null;
    }

    //TODO:use
    public long waitAckCnt() {
        return waitAckCnt.get();
    }

    protected void preDump(MysqlConnection connection) {
        if (binlogParser != null && binlogParser instanceof LogEventConvert) {
            metaConnection = (MysqlConnection) connection.fork();
            try {
                metaConnection.connect();
            } catch (IOException e) {
                throw new CanalParseException(e);
            }

            BinlogFormat[] supportBinlogFormats = agentConf.getDbSyncSupportBinlogFormats();
            if (supportBinlogFormats != null && supportBinlogFormats.length > 0) {
                BinlogFormat format = metaConnection.getBinlogFormat();
                boolean found = false;
                for (BinlogFormat supportFormat : supportBinlogFormats) {
                    if (supportFormat != null && format == supportFormat) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogFormat " + format);
                }
            }

            BinlogImage[] supportBinlogImages = agentConf.getDbSyncBinlogImages();
            if (supportBinlogImages != null && supportBinlogImages.length > 0) {
                BinlogImage image = ((MysqlConnection) metaConnection).getBinlogImage();
                boolean found = false;
                for (BinlogImage supportImage : supportBinlogImages) {
                    if (supportImage != null && image == supportImage) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogImage " + image);
                }
            }

            if (isSwitching.get()) {
                inSwitchParseDone.compareAndSet(true, false);
                isSwitching.compareAndSet(true, false);
            }

            tableMetaCache = new TableMetaCache(metaConnection);
            ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
            clearCache();
            if (dispatcherThread != null && parseThreadList.size() <= 0) {
                LOGGER.info("{} build binlog parser!", this.jobName);
                for (int i = 0; i < 20; i++) {
                    ParseThread parser = new ParseThread("parser-"
                            + i + "-" + this.jobName, metaConnection, this);
                    parser.start();
                    //parseThreadList.add(parser);
                    parseThreadList.put(i, parser);
                }
            } else {
                try {
                    metaConnection.disconnect();
                } catch (IOException e) {
                    throw new CanalParseException(e);
                }
            }
            LOGGER.info("TableMetaCache use MysqlConnection port : {} : {}", jobName, metaConnection.getLocalPort());
        }
    }

    public void clearCache() {
        sendLogPositionCache.clear();
        eventLogPositionCache.clear();
    }

    private RelayLog mkRelayLog() {
        String relogRoot = agentConf.get(DBSYNC_RELAY_LOG_ROOT, DEFAULT_DBSYNC_RELAY_LOG_ROOT).trim();
        String relayLogWay = agentConf.get(DBSYNC_RELAY_LOG_WAY);
        RelayLog log = null;
        if (relayLogWay != null && relayLogWay.trim().endsWith("memory")) {
            int memSize = agentConf.getInt(DBSYNC_RELAY_LOG_MEM_SZIE);
            log = new MemRelayLogImpl(memSize * 1024 * 1024L);
        } else {
            String jobRelyLogPath = null;
            String jobPrefix = mysqlAddress.replace('.', '-') + "-" + mysqlPort;
            if (relogRoot.endsWith("/")) {
                jobRelyLogPath = relogRoot + jobPrefix + "/";
            } else {
                jobRelyLogPath = relogRoot + "/" + jobPrefix + "/";
            }
            log = new AsyncRelayLogImpl(jobPrefix, jobRelyLogPath);
        }

        return log;
    }

    private Thread mkDispatcher() {
        Thread dispatcher = new Thread(() -> {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            LOGGER.info("{} dispatcher thread begin work!", jobName);

            boolean bInTransaction = false;
            parseDisptcherStatus = State.RUN;
            int index = 0;
            boolean sendLock = false;

            if (lastLog != null) {
                parseLastLog = new LogPosition(lastLog);
            }

            while (dispatchRunning || sendLock) {

                if (isSwitching.get()) {
                    relayLog.clearLog();
                    bufferList.clear();
                    bInTransaction = false;
                    inSwitchParseDone.compareAndSet(false, true);
                    DBSyncUtils.sleep(1);
                    continue;
                }

                LogEvent event = null;
                byte[] logBodyData = relayLog.getLog();
                if (null == logBodyData) {
                    if (dispatchRunning || sendLock) {
                        DBSyncUtils.sleep(1);
                        continue;
                    } else {
                        break;
                    }
                } else if (logBodyData.length == 4 && isValidCheckSum(logBodyData)) {
                    int checkSum = calcCheckSum(logBodyData);
                    context.setFormatDescription(new FormatDescriptionLogEvent(4, checkSum));
                    continue;
                } else {
                    try {
                        LogBuffer buffer = new LogBuffer(logBodyData, 0, logBodyData.length);
                        //won't send event, update gtid directly, or else update gtid when ack
                        event = decoder.decode(buffer, context);
                        if (event == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        LOGGER.error("decode event encounter a error: ", e);
                    }
                }

                try {
                    heartBeat = System.currentTimeMillis();
                    long eventTmStp = event.getWhen();

                    synchronized (syncObject) {
                        LogHeader logHead = event.getHeader();
                        if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                            String logfileName = ((RotateLogEvent) event).getFilename();
                            parseLastLog.getPosition().setJournalName(logfileName);
                        }
                        if (logHead.getLogPos() != 0 && logHead.getLogPos() != logHead.getEventLen()) {
                            parseLastLog.getPosition().setPosition(event.getLogPos());
                        }
                        parseLastLog.getPosition().setTimestamp(event.getWhen() * 1000);
                    }

                    Entry entry = null;
                    if (event.getHeader().getType() == LogEvent.XID_EVENT
                            || event.getHeader().getType() == LogEvent.QUERY_EVENT) {
                        entry = parseAndProfilingIfNecessary(event, jobconf);

                        if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                            bufferList.add(event);
                            bInTransaction = true;
                        } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                            // end package binlog event
                            bufferList.add(event);
                            // dispatcher transaction to a parser
                            ParseThread parser = null;
                            int tmpIndex = index;
                            do {
                                parser = parseThreadList.get(index);
                                index = (index + 1) % parseThreadList.size();
                                if (index == tmpIndex) {
                                    DBSyncUtils.sleep(1);
                                }
                            } while (parser != null && parser.bParseQueueIsFull() && dispatchRunning);

                            pkgIndexId = genIndexOrder(eventTmStp, bufferList.size() + 1);
                            if (parser != null) {
                                parser.putEvents(new PkgEvent(bufferList, pkgIndexId, true));
                            }
                            //pkgIndexId = pkgIndexId + bufferList.size();
                            bufferList = new ArrayList<>();
                            bInTransaction = false;
                            if (sendLock) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("{} dispatcher free lock to {} parser!",
                                            jobName, index);
                                }
                            }
                            sendLock = false;
                        } else {
                            // this is not a transactionend query
                            // event
                            ArrayList<LogEvent> tmpEvenList = new ArrayList<>();
                            tmpEvenList.add(event);
                            for (ParseThread parser : parseThreadList.values()) {
                                pkgIndexId = genIndexOrder(eventTmStp, 1);
                                parser.putEvents(new PkgEvent(tmpEvenList, pkgIndexId, true));
                            }
                            //pkgIndexId = pkgIndexId + parseThreadList.size();
                        }
                    } else if (bInTransaction) {
                        bufferList.add(event);
                        // if bufferList size to large need lock a
                        // process
                        if (bufferList.size() > 20) {
                            ParseThread parser = null;
                            if (sendLock) {
                                parser = parseThreadList.get(index);
                                while (parser != null && parser.bParseQueueIsFull()) {
                                    DBSyncUtils.sleep(1);
                                }
                            } else {
                                sendLock = true;
                                int tmpIndex = index;
                                do {
                                    index = (index + 1) % parseThreadList.size();
                                    parser = parseThreadList.get(index);
                                    if (index == tmpIndex) {
                                        DBSyncUtils.sleep(100);
                                    }
                                } while (parser != null && parser.bParseQueueIsFull() && dispatchRunning);
                            }
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("{} dispatcher lock send data to {} parser!", jobName, index);
                            }
                            pkgIndexId = genIndexOrder(eventTmStp, bufferList.size() + 1);
                            if (parser != null) {
                                parser.putEvents(new PkgEvent(bufferList, pkgIndexId, false));
                            }
                            //pkgIndexId = pkgIndexId + bufferList.size();
                            bufferList = new ArrayList<LogEvent>();
                        }

                    } else {
                        // dispatche the message to all the parser
                        ArrayList<LogEvent> tmpEvenList = new ArrayList<LogEvent>();
                        tmpEvenList.add(event);
                        for (ParseThread parser : parseThreadList.values()) {
                            pkgIndexId = genIndexOrder(eventTmStp, 1);
                            parser.putEvents(new PkgEvent(tmpEvenList, pkgIndexId, true));
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("", e);
                } catch (Exception e) {
                    LOGGER.error("{} async dispatcher Error : {}", jobName, DBSyncUtils.getExceptionStack(e));
                }
            }
            LOGGER.info("Job : " + jobName + " dispatcher Thread stopped!");
            if (parseLastLog != null) {
                LOGGER.warn("{}, dispatcher stop position : {}",
                        jobName, parseLastLog.getJsonObj().toString());
            }
            /*
             * in case that dump thread won't be properly stopped
             */
            inSwitchParseDone.compareAndSet(false, true);

            parseDisptcherStatus = State.STOP;
        });

        dispatcher.setName(jobName + "-dispatcher");
        dispatcher.setUncaughtExceptionHandler((t, e) -> {
            setJobStat(State.STOP);
            //TODO:handle monitor
//            MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
//                    ErrorCode.OP_EXCEPTION_DISPATCH_THREAD_EXIT_UNCAUGHT);
            setErrorMsg(e.getMessage());
            LOGGER.error("{} dispatcher Thread has an uncaught error {}",
                    jobName, DBSyncUtils.getExceptionStack(e));
        });

        return dispatcher;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void ackSendPosition(LogPosition logPosition) {
        if (logPosition != null) {
            try {
                ackLogPositionList.put(logPosition);
            } catch (InterruptedException e) {
                LOGGER.error("ackLogPositionList has exception e = {}", e);
            }
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ackSendPosition is null");
            }
        }
    }

    public void addEventLogPosition(LogPosition eventLogPosition) {
        eventLogPositionCache.add(eventLogPosition);
    }

    public void removeEventLogPosition(LogPosition eventLogPosition) {
        eventLogPositionCache.remove(eventLogPosition);
    }

    protected Entry parseAndProfilingIfNecessary(LogEvent bod, DBSyncJobConf jobconf) throws Exception {
        Entry event = binlogParser.parse(bod, jobconf);
        return event;
    }

    public LogPosition getMinCacheEventLogPosition() {
        if (status != State.RUN) {
            return null;
        }
        minEventLogPosition = getMaxProcessedPosition();
        try {
            minEventLogPosition = eventLogPositionCache.first();
        } catch (Exception e) {
            if (!(e instanceof NoSuchElementException)) {
                LOGGER.warn("getMinCacheEventLogPosition has exception e = {}", e);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("getMinCacheEventLogPosition has exception e = {}", e);
                }
            }
        }
        return minEventLogPosition;
    }

    public LogPosition getMinCacheSendLogPosition() {
        if (status != State.RUN) {
            return null;
        }
        if (sendLogPositionCache.size() > 0) {
            LogPosition logPosition = null;
            try {
                logPosition = sendLogPositionCache.first();
            } catch (Exception e) {
                LOGGER.error("getMinCacheLogPosition has exception e = {}", e);
            }
            if (logPosition != null) {
                return new LogPosition(logPosition);
            }
        }
        return null;
    }

    public BinlogParser getBinlogParser() {
        return binlogParser;
    }

    public String getJobName() {
        return jobName;
    }

    protected void addLogPositionToCache(LogPosition logPosition) {
        try {
            long currentTimeStamp = Instant.now().toEpochMilli();
            semaphore.acquire();
            sendLogPositionCache.add(logPosition);
            if (LOGGER.isDebugEnabled()
                    && ((currentTimeStamp - lastTimeStamp) > printStatIntervalMs)) {
                LOGGER.debug("[{}], logPosition {}, cacheLogPosition position in cache [{}],  cache"
                                + "availablePermits = {}",
                        jobName, logPosition, sendLogPositionCache.size(), semaphore.availablePermits());
                lastTimeStamp = currentTimeStamp;
            }
        } catch (Exception e) {
            LOGGER.error("cacheLogPosition has exception e = ", e);
        }
    }

    private Thread mkDumpThread() {
        Thread dumperThread = new Thread(() -> {
            MysqlConnection erosaConnection = null;
            boolean bNeedClearParseThread = false;
            while (running) {
                InetSocketAddress dbConnectionAddress = null;
                try {
                    if (needReset.get()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("begin to start reset to master");
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_RESET);
                        if (doReset()) {
                            LOGGER.info("Job {} reset to master ok, new reBuild Relay-log!", jobName);
                            relayLog.close();
                            relayLog = mkRelayLog();
                            bNeedClearParseThread = true;
                        } else {
                            LOGGER.info("Job {} reset to master failed", jobName);
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(),
                                MonitorLogUtils.JOB_STAT_RESET_FINISHED);
                        needReset.set(false);
                    }

                    // reach max reconnect, start HA
                    if (reConnectCnt > doSwitchCnt) {
                        // reset reconnect num
                        reConnectCnt = 0;
                        isSwitching.compareAndSet(false, true);
                        bNeedClearParseThread = true;
                        while (!inSwitchParseDone.get()) {
                            DBSyncUtils.sleep(1);
                        }
                        // set binlog parse position as dump position
                        synchronized (syncObject) {
                            LogPosition tmpPos = getMaxParseLogPosition();
                            if (tmpPos != null) {
                                lastLog = tmpPos;
                                LOGGER.info("Job {} HA reset pos to {}!", jobName, tmpPos.getJsonObj().toJSONString());
                            }
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(),
                                MonitorLogUtils.JOB_STAT_SWITCH);
                        if (doSwitch()) {
                            LOGGER.info("Job {} switch ok, new reBuild Relay-log!", jobName);
                            relayLog.close();
                            relayLog = mkRelayLog();
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(),
                                MonitorLogUtils.JOB_STAT_SWITCH_FINISHED);
                    }

                    if (bNeedClearParseThread) {
                        bNeedClearParseThread = false;
                        if (parseThreadList != null && parseThreadList.size() > 0) {
                            for (ParseThread parser : parseThreadList.values()) {
                                parser.stopParse();
                            }
                            parseThreadList.clear();
                            LOGGER.info("Job {} clear binlog parser!", jobName);
                        }
                    }

                    // start execute replication
                    // 1. construct Erosa conn
                    erosaConnection = buildErosaConnection(mysqlAddress, mysqlPort);
                    erosaConnection.connect();

                    // whether masterAddr changes after last exception
                    String newRemoteIp = erosaConnection.getConnector().getRemoteAddress();
                    dbConnectionAddress = erosaConnection.getConnector().getAddress();
                    if (dbConnectionAddress != null) {
                        updateCurrentDbInfo(dbConnectionAddress.toString());
                    }
                    if (remoteIp != null) {
                        if (newRemoteIp != null && !newRemoteIp.equalsIgnoreCase(remoteIp)) {
                            LOGGER.info("Find database change, so flush old LogPosition : {}",
                                    lastLog.getJsonObj().toJSONString());
                            remoteIp = newRemoteIp;
                            masterPosition = null;
                            masterInfo = new AuthenticationInfo(dbConnectionAddress, userName, passwd);
                        }
                    } else {
                        remoteIp = newRemoteIp;
                        masterInfo = new AuthenticationInfo(dbConnectionAddress, userName, passwd);
                    }
                    LOGGER.info("Dumper use MysqlConnection JobName/mysqlAddress/remoteIp/port : "
                            + "{}/{}/{}/{}", jobName, mysqlAddress, remoteIp, erosaConnection.getLocalPort());
                    // 3. get last position info
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_GET_POSITION);
                    final EntryPosition startPosition = findStartPosition(erosaConnection);
                    if (startPosition == null) {
                        LOGGER.error("{} can't find start position", jobName);
                        throw new CanalParseException("can't find start position");
                    }
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(),
                            MonitorLogUtils.JOB_STAT_GET_POSITION_FINISHED, "",
                            startPosition.getJsonObj().toJSONString());
                    LOGGER.info("job {}, find start position : {}", jobName, startPosition);
                    // reconnect
                    erosaConnection.reconnect();
                    LOGGER.info("Dumper use MysqlConnection reconnect JobName/mysqlAddress/remoteIp/port : "
                            + "{}/{}/{}/{}", jobName, mysqlAddress, remoteIp, erosaConnection.getLocalPort());

                    final LogPosition lastPosition = new LogPosition();
                    lastPosition.setPosition(startPosition);
                    lastPosition.setIdentity(new LogIdentity(dbConnectionAddress, -1L));
                    minEventLogPosition = new LogPosition(lastPosition);
                    if (parseLastLog == null) {
                        parseLastLog = new LogPosition(lastPosition);
                    }
                    if (lastLog == null) {
                        lastLog = new LogPosition(lastPosition);
                    }

                    @SuppressWarnings("rawtypes") final SinkFunction sinkHandler = (SinkFunction<LogEvent>) event -> {

                        if (needReset.get()) {
                            //break the loop
                            return false;
                        }

                        LogHeader logHead = event.getHeader();

                        if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            // skip heartbeat event
                            return running;
                        }
                        if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                            // skip fake rotate event
                            String newJournalName = ((RotateLogEvent) event).getFilename();
                            lastPosition.getPosition().setJournalName(newJournalName);
                        }

                        if (logHead.getLogPos() != 0 && logHead.getLogPos() != logHead.getEventLen()) {
                            lastPosition.getPosition().setPosition(logHead.getLogPos() - logHead.getEventLen());
                        }

                        lastPosition.getPosition().setTimestamp(logHead.getWhen() * 1000);
                        // record positions
                        flushLastPosition(lastPosition);

                        return running;
                    };

                    // success connection, reset conn num
                    reConnectCnt = 0;
                    setState(State.RUN);
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(),
                            MonitorLogUtils.JOB_STAT_DUMP_RUN);
                    if (stopHb != null) {
                        JSONObject tmpHb = new JSONObject(stopHb);
                        stopHb = null;
//                        monitor.putStopHeartbeat(tmpHb); //TODO: add stophb to jobmonitor
                    }
                    // 2. predump operation
                    preDump(erosaConnection);
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_FETCH_DATA);
                    erosaConnection.seekAndCopyData(startPosition.getJournalName(), startPosition.getPosition(),
                            sinkHandler, relayLog);
                } catch (RelayLogPosErrorException re) {
                    setState(State.ERROR);
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                            ErrorCode.DB_EXCEPTION_RELAY_LOG_POSITION);
                    setErrorMsg(re.getMessage());

                    synchronized (syncObject) {
                        lastLog = new LogPosition(parseLastLog);
                    }
                    LOGGER.error("JobName [{}] relay-log file magic error : ", jobName, re);
                    LOGGER.error("JobName [{}] relay-log use RedumpPosition is : {} ",
                            jobName, parseLastLog);
                } catch (TableIdNotFoundException e) {
                    setState(State.ERROR);
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                            ErrorCode.DB_EXCEPTION_TABLE_ID_NOT_FOUND);
                    setErrorMsg(e.getMessage());

                    needTransactionPosition.compareAndSet(false, true);
                    LOGGER.error("JobName [{}] dump address {} has an error, retrying. caused by",
                            jobName, getDbIpAddress(dbConnectionAddress), e);
                } catch (BinlogMissException e) {
                    setState(State.ERROR);
                    MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                            ErrorCode.DB_EXCEPTION_BINLOG_MISS);
                    setErrorMsg(e.getMessage());
                    LOGGER.error("JobName [{}] Dump {} Error : ",
                            jobName, getDbIpAddress(dbConnectionAddress), e);
                    LOGGER.error("JobName [{}] Flush old position log : {}",
                            jobName, lastLog.getJsonObj().toJSONString());
                    //lastLog = null;
                    bBinlogMiss = true;
                    String alarmMsg = e.getMessage() + "\nBegin dump from the max position \nOld position ";
//                    sendAlarm(getDbIpAddress(dbConnectionAddress), alarmMsg); //TODO:complete?
                } catch (Throwable e) {
                    if (!running) {
                        if (!(e instanceof java.nio.channels.ClosedByInterruptException
                                || e.getCause() instanceof java.nio.channels.ClosedByInterruptException)) {
                            throw new CanalParseException(String.format("dump address %s has an error, retrying. ",
                                    getDbIpAddress(dbConnectionAddress)), e);
                        }
                    } else {
                        setState(State.ERROR);
                        MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                                ErrorCode.DB_EXCEPTION_OTHER);
                        setErrorMsg(e.getMessage());
                        if (running || LOGGER.isDebugEnabled()) {
                            LOGGER.error("JobName [{}] dump address {} has an error, retrying. caused by ",
                                    jobName, getDbIpAddress(dbConnectionAddress), e);
                        }
                    }
                } finally {
                    // close connection
                    reConnectCnt++;
                    afterDump(erosaConnection);
                    try {
                        if (erosaConnection != null) {
                            erosaConnection.disconnect();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Dump Connection is closed : {}, {}",
                                        erosaConnection.getLocalPort(), reConnectCnt);
                            }
                        }
                    } catch (IOException e1) {
                        if (!running) {
                            throw new CanalParseException(
                                    String.format("disconnect address %s has an error, retrying %d. ",
                                            getDbIpAddress(dbConnectionAddress), reConnectCnt), e1);
                        } else {
                            LOGGER.error("disconnect address {} has an error, retrying {}., "
                                            + "caused by ",
                                    getDbIpAddress(dbConnectionAddress), reConnectCnt, e1);
                        }
                    }

                    DBSyncUtils.sleep(reConnectCnt * 1000L);
                }
            }

            LOGGER.info("Job : " + jobName + " dump Thread stopped!");
            setState(State.STOP);
            MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
                    ErrorCode.OP_EXCEPTION_DUMPER_THREAD_EXIT);
        });
        dumperThread.setName(jobName + "-dumper");
        dumperThread.setPriority(Thread.MAX_PRIORITY);
        dumperThread.setUncaughtExceptionHandler((t, e) -> {
            setState(State.STOP);
            setErrorMsg(e.getMessage());
            MonitorLogUtils.printJobStat(this.getCurrentDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
                    ErrorCode.OP_EXCEPTION_DUMPER_THREAD_EXIT_UNCAUGHT);
            LOGGER.error("{} dump Thread has an uncaught error ", jobName, e);
        });

        return dumperThread;
    }

    protected void afterDump(MysqlConnection connection) {

        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
                metaConnection = null;
            } catch (IOException e) {
                LOGGER.error("ERROR # disconnect for address:{}", metaConnection.getConnector().getAddress(), e);
            }
        }
    }

    private void doResetFinal() {
        String dbIp = jobconf.getMstMysqlIp();
        int dbPort = jobconf.getMstMysqlPort();

        this.remoteIp = null;

        this.mysqlAddress = dbIp;
        this.mysqlPort = dbPort;

        //TODO: monitor operate
//        this.stopHb = monitor.getLastHbInfo(this.jobName);
        this.jobconf.doReset();
//        this.manager.updateJob(jobconf.getJobName(), this.jobName, TaskStat.NORMAL);
//        this.monitor.doSwitchMonitor(jobconf.getJobName(), this.jobName);
        this.jobName = jobconf.getJobName();
        this.masterInfo = new AuthenticationInfo(new InetSocketAddress(mysqlAddress, mysqlPort), userName, passwd);
    }

    /**
     * open ha swtich
     *
     * @throws IOException
     */
    private boolean doSwitch() throws IOException {
        String bakIp = jobconf.getBakMysqlIp();
        int bakPort = jobconf.getBakMysqlPort();
        if (bakIp == null || bakIp.trim().equalsIgnoreCase("") || -1 == bakPort) {
            LOGGER.warn("jobName can't do HA switch , back database : {}:{}", bakIp, bakPort);
            return false;
        }
        LOGGER.warn("jobName begin HA switch , back database : {}:{}", bakIp, bakPort);
        if (lastEntryList != null && this.lastEntry == null && lastEntryList.size() > 0) {
            lastEntry = lastEntryList.get(0);
            LOGGER.info("{} HA switch last entry size : {}", this.jobName, lastEntryList.size());
        }
        LOGGER.info("{} HA switch last pos : {}", this.jobName, this.lastParsePos);

        if (this.lastParsePos != null && this.lastEntry != null) {
            MysqlConnection bakConnection = buildErosaConnection(bakIp, bakPort);
            bakConnection.connect();
            LogPosition startPosition = checkHAStartFileName(bakConnection);
            if (startPosition != null) {
                LOGGER.warn("jobName HA switch , back database : {}:{} find start binlog : {}",
                        bakIp, bakPort, startPosition);
                LogPosition bakPosition = findHAstartPosition(bakConnection, startPosition);
                lastLog.setPosition(bakPosition.getPosition());
                lastLog.setIdentity(bakPosition.getIdentity());
                LOGGER.warn("jobName end HA switch , find back database : {}:{}, start log : {}!",
                        bakIp, bakPort, bakPosition);
            } else {
                lastLog = null;
                LOGGER.error("jobName end HA switch , find back database : {}:{},  not find start binlog fileName!",
                        bakIp, bakPort);
            }
        } else {
            LOGGER.error("Because the first Connection : {}:{} error; jobName HA switch, find back database : {}:{}!",
                    this.mysqlAddress, this.mysqlPort, bakIp, bakPort);
        }
        doSwitchFinal();
        return true;
    }

    private void doSwitchFinal() {
        String bakIp = jobconf.getBakMysqlIp();
        int bakPort = jobconf.getBakMysqlPort();

        this.remoteIp = null;

        this.mysqlAddress = bakIp;
        this.mysqlPort = bakPort;
//        this.stopHb = monitor.getLastHbInfo(this.jobName);
        this.jobconf.doSwitch();
//        this.manager.updateJob(jobconf.getJobName(), this.jobName, TaskStat.SWITCHED);
//        this.monitor.doSwitchMonitor(jobconf.getJobName(), this.jobName);
        this.jobName = jobconf.getJobName();
        this.masterInfo = new AuthenticationInfo(new InetSocketAddress(mysqlAddress, mysqlPort), userName, passwd);
    }

    /**
     * reset collect position
     *
     * @return reset true if reset succeed or false if reset failed
     * @throws IOException
     */
    private boolean doReset() throws IOException {
        String dbIp = jobconf.getMstMysqlIp();
        int dbPort = jobconf.getMstMysqlPort();
        if (Objects.equals(dbIp, mysqlAddress) && Objects.equals(dbPort, mysqlPort)) {
            LOGGER.info("{}:{} is being dumping, no need to reset", dbIp, dbPort);
            resetFuture.complete(null);
            return true;
        }

        if (StringUtils.isBlank(dbIp) || -1 == dbPort) {
            LOGGER.warn("{} can't reset, invalid database: {}:{}", this.jobName, dbIp, dbPort);
            jobconf.setStatus(TaskStat.SWITCHED);
            resetFuture.completeExceptionally(new DataSourceConfigException(
                    "invalid config, database: " + dbIp + ":" + dbPort));
            return false;
        }

        LOGGER.info("begin switch to master database : {}:{}", dbIp, dbPort);
        if (lastEntryList != null && this.lastEntry == null && lastEntryList.size() > 0) {
            lastEntry = lastEntryList.get(0);
            LOGGER.info("{} switch to master, last entry size: {}, ", this.jobName, lastEntryList.size());
        }
        LOGGER.info("{} switch to master, last pos: {}", this.jobName, this.lastParsePos);

        try {
            if (this.lastParsePos != null && this.lastEntry != null) {
                MysqlConnection masterConnection = buildErosaConnection(dbIp, dbPort);
                masterConnection.connect();
                LogPosition startPosition = checkHAStartFileName(masterConnection);
                if (startPosition != null) {
                    LOGGER.warn("{} switch to master , database : {}:{} find start binlog : {}",
                            this.jobName, dbIp, dbPort, startPosition.toString());
                    LogPosition bakPosition = findHAstartPosition(masterConnection, startPosition);
                    lastLog.setPosition(bakPosition.getPosition());
                    lastLog.setIdentity(bakPosition.getIdentity());
                    LOGGER.warn("{} switch to master {}:{} finished, start position: {}!",
                            this.jobName, dbIp, dbPort, bakPosition.toString());
                } else {
                    lastLog = null;
                    LOGGER.error("{} switch to master {}:{} finished, not find start binlog fileName!",
                            this.jobName, dbIp, dbPort);
                }
            } else {
                LOGGER.error("Because the first Connection : {}:{} error to switch to master {}:{}!",
                        this.mysqlAddress, this.mysqlPort, dbIp, dbPort);
            }
        } catch (Exception e) {
            LOGGER.error("reset exception: ", e);
            jobconf.setStatus(TaskStat.SWITCHED);
            resetFuture.completeExceptionally(e);
            return false;
        }

        doResetFinal();
        resetFuture.complete(null);
        return true;
    }

    private LogPosition findHAstartPosition(MysqlConnection connection, LogPosition startPostion) {
        final long logTimeStample = lastParsePos.getPosition().getTimestamp() / 1000;
        final long maxEndlogTime = logTimeStample + fallbackIntervalInSeconds;
        final LogPosition searchPosition = new LogPosition(startPostion);

        int reConnectCnt = 0;
        final LogPosition firstPosition = new LogPosition(startPostion);
        MysqlConnection tmpMetaConnection = null;
        final LogEventConvert convert = new LogEventConvert();
        while (reConnectCnt < 20) {
            preDump(connection);
            try {

                tmpMetaConnection = connection.fork();
                tmpMetaConnection.reconnect();
                TableMetaCache tmpMetaCache = new TableMetaCache(tmpMetaConnection);
                convert.setTableMetaCache(tmpMetaCache);
                convert.setNameFilter(null);

                connection.reconnect();
                connection.dump(searchPosition.getPosition().getJournalName(),
                        searchPosition.getPosition().getPosition(),
                        new SinkFunction<LogEvent>() {

                            private boolean bReachFirst = true;
                            private int compareEventCnt = 0;
                            private ArrayList<Entry> tmpEntryLists;
                            private boolean bFoundFirstTimeStampleLog = false;

                            public boolean sink(LogEvent event) {

                                if (event.getHeader().getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                                    return false;
                                }

                                if (event.getHeader().getType() == LogEvent.ROTATE_EVENT
                                        || event.getHeader().getType() == LogEvent.TABLE_MAP_EVENT) {
                                    convert.parse(event, jobconf);
                                }

                                if ((event.getHeader().getWhen() < maxEndlogTime)) {
                                    Entry entry = null;
                                    try {
                                        entry = convert.parse(event, jobconf);
                                        if (entry != null) {
                                            if (event.getHeader().getWhen() == logTimeStample) {
                                                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                                                    compareEventCnt = 0;
                                                    if (!bFoundFirstTimeStampleLog) {
                                                        bFoundFirstTimeStampleLog = true;
                                                        EntryPosition entryPos = new EntryPosition(
                                                                entry.getHeader().getLogfileName(),
                                                                entry.getHeader().getLogfileOffset());
                                                        entryPos.setTimestamp(event.getHeader().getWhen() * 1000);
                                                        firstPosition.setPosition(entryPos);
                                                    }
                                                } else {
                                                    if (compareEventCnt >= lastEntryList.size()
                                                            && entry.getEntryType() == EntryType.TRANSACTIONEND) {
                                                        EntryPosition entryPos = new EntryPosition(
                                                                entry.getHeader().getLogfileName(),
                                                                entry.getHeader().getLogfileOffset());
                                                        entryPos.setTimestamp(event.getHeader().getWhen() * 1000);
                                                        firstPosition.setPosition(entryPos);
                                                        return false;
                                                    }
                                                    if (compareEventCnt < lastEntryList.size()
                                                            && compareEntry(entry,
                                                            lastEntryList.get(compareEventCnt))) {
                                                        compareEventCnt++;
                                                    } else {
                                                        compareEventCnt = 0;
                                                    }
                                                }

                                            }

                                            searchPosition.getPosition()
                                                    .setJournalName(entry.getHeader().getLogfileName());
                                            searchPosition.getPosition()
                                                    .setPosition(entry.getHeader().getLogfileOffset());
                                            searchPosition.getPosition()
                                                    .setTimestamp(event.getHeader().getWhen() * 1000);

                                        }
                                    } catch (Exception e) {
                                        LOGGER.error("Search position {} exception: ",
                                                searchPosition.getPosition().getJsonObj().toJSONString(), e);
                                    }
                                } else {
                                    // not position
                                    return false;
                                }
                                return true;
                            }
                        });
                break;
            } catch (IOException ie) {
                reConnectCnt++;
                LOGGER.error("Search {}:{} binlog file {} exception : {} ",
                        jobconf.getBakMysqlIp(), jobconf.getBakMysqlPort(),
                        startPostion.getPosition().getJournalName(),
                        DBSyncUtils.getExceptionStack(ie));
            } finally {
                if (tmpMetaConnection != null) {
                    try {
                        tmpMetaConnection.disconnect();
                    } catch (Exception e) {
                        LOGGER.error("", e);
                    }
                }
            }
        }

        return firstPosition;
    }

    protected boolean compareEntry(Entry entry1,
            Entry entry2) {

        if (!entry1.getHeader().getSchemaName().equals(entry2.getHeader().getSchemaName())) {
            return false;
        }

        if (!entry1.getHeader().getTableName().equals(entry2.getHeader().getTableName())) {
            return false;
        }

        try {
            RowChange rowChage1 = RowChange.parseFrom(entry1.getStoreValue());
            RowChange rowChage2 = RowChange.parseFrom(entry2.getStoreValue());
            if (rowChage1.getEventType() != rowChage2.getEventType()
                    || rowChage1.getRowDatasList().size() != rowChage2.getRowDatasList().size()) {
                return false;
            }

            // rowChage1.get
            for (int i = 0; i < rowChage1.getRowDatasList().size(); i++) {
                RowData row1 = rowChage1.getRowDatasList().get(i);
                RowData row2 = rowChage2.getRowDatasList().get(i);
                List<Column> beforCloum1 = row1.getBeforeColumnsList();
                List<Column> afterCloum1 = row1.getAfterColumnsList();

                List<Column> beforCloum2 = row2.getBeforeColumnsList();
                List<Column> afterCloum2 = row2.getAfterColumnsList();

                if (beforCloum1.size() != beforCloum2.size()) {
                    return false;
                }

                for (int j = 0; j < beforCloum1.size(); j++) {
                    if (!Objects.equals(beforCloum1.get(j), beforCloum2.get(j))) {
                        return false;
                    }
                }

                if (afterCloum1.size() != afterCloum2.size()) {
                    return false;
                }

                for (int k = 0; k < afterCloum1.size(); k++) {
                    if (!Objects
                            .equals(afterCloum1.get(k).getValue(), afterCloum2.get(k).getValue())) {
                        return false;
                    }
                }
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("parse rowdata exception: ", e);
        }
        return true;
    }

    private LogPosition checkHAStartFileName(MysqlConnection connection) {
        final List<String> binlogFileNameList = new ArrayList<>();
        EntryPosition endPosition = findEndPosition(connection);
        EntryPosition startPosition = findStartPositionCmd(connection);
        String maxBinlogFileName = endPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();

        final LogPosition searchPosition = new LogPosition();
        LogIdentity identity = new LogIdentity(connection.getConnector().getAddress(), -1L);
        searchPosition.setIdentity(identity);

        String binlogFileNamePrefix = maxBinlogFileName.substring(0, maxBinlogFileName.lastIndexOf(".") + 1);
        int binlogMaxSeqNum = Integer.parseInt(maxBinlogFileName.substring(maxBinlogFileName.lastIndexOf(".") + 1));

        int binlogMinSeqNum = Integer.parseInt(minBinlogFileName.substring(minBinlogFileName.lastIndexOf(".") + 1));
        for (int index = binlogMaxSeqNum; index >= binlogMinSeqNum; index--) {
            binlogFileNameList.add(binlogFileNamePrefix + String.format("%06d", index));
        }

        final long masterEndlogTime = lastParsePos.getPosition().getTimestamp() / 1000 - fallbackIntervalInSeconds;
        final InBoxBoolean bFoundPos = new InBoxBoolean();
        bFoundPos.bFound = false;
        LOGGER.info("Get {}:{} binlog file name : {}, time stample : {}", jobconf.getBakMysqlIp(),
                jobconf.getBakMysqlPort(), binlogFileNameList, masterEndlogTime);
        for (int i = 0; i < binlogFileNameList.size(); i++) {
            String binFileLogName = binlogFileNameList.get(i);
            EntryPosition entryPos = new EntryPosition(binFileLogName, 4L);
            LOGGER.info("{} seek binlog file : {}", this.jobName, binFileLogName);
            searchPosition.setPosition(entryPos);
            try {
                connection.reconnect();
                connection.seek(binFileLogName, 4L, (new SinkFunction<LogEvent>() {
                    private int pos;
                    private boolean bFirstEvent = true;
                    private boolean bFirstRotateEvent = true;

                    public SinkFunction<LogEvent> setLogPos(int pos) {
                        this.pos = pos;
                        return this;
                    }

                    public boolean sink(LogEvent event) {

                        LogHeader logHead = event.getHeader();
                        if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            //skip heartbeat event
                            return true;
                        }
                        if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                            // skip fake rotate event
                            if (logHead.getWhen() == 0 && logHead.getLogPos() == 0 && logHead.getFlags() == 0x20) {
                                //go on with next event in the same binlog file
                                return true;
                            }
                        }

                        long nowTmStample = event.getHeader().getWhen();
                        //check the timestamp of each binlog file' first event
                        //if they are both larger than dump time
                        //need the latter binlog file
                        if (bFirstEvent) {
                            if (nowTmStample > (masterEndlogTime + fallbackIntervalInSeconds)) {
                                return false;
                            }
                            bFirstEvent = false;
                        }

                        if (event.getHeader().getType() == LogEvent.ROTATE_EVENT) {
                            if (!bFirstRotateEvent) {
                                pos = pos - 1;
                                if (pos < 0) {
                                    return false;
                                }

                                String journalName = binlogFileNameList.get(pos);
                                searchPosition.getPosition().setJournalName(journalName);
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("{} job seek start pos, find rotate event in filename {}",
                                            jobName, journalName);
                                }
                            } else {
                                bFirstRotateEvent = false;
                            }
                        }
                        if (nowTmStample >= masterEndlogTime) {
                            searchPosition.getPosition().setPosition(event.getHeader().getLogPos());
                            searchPosition.getPosition().setTimestamp(nowTmStample * 1000);
                            searchPosition.getPosition().setServerId(event.getServerId());
                            bFoundPos.bFound = true;
                            return false;
                        }
                        return true;
                    }
                }).setLogPos(i));

                if (bFoundPos.bFound) {
                    break;
                }

            } catch (Throwable e) {
                LOGGER.error("Find start binlog file name error : {}", DBSyncUtils.getExceptionStack(e));
            }
        }

        if (bFoundPos.bFound) {
            return searchPosition;
        }

        if (agentConf.getBoolean(DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS,
                DEFAULT_DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS)) {
            /*
             * return the first binlog position exist in back db
             */
            LOGGER.error("{} cannot find binlog, so read from start", jobName);

            searchPosition.setPosition(startPosition);
            return searchPosition;
        }
        return null;
    }

    public CompletableFuture<Void> resetJob() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (!needReset.compareAndSet(false, true)) {
            future.completeExceptionally(new JobInResetStatus("job " + jobName + " is being resetting"));
        } else {
            resetFuture = future;
        }
        return future;
    }

    public String getCurrentDbInfo() {
        if (StringUtils.isEmpty(currentDbInfo)) {
            currentDbInfo = jobconf.getJobName();
        }
        return currentDbInfo;
    }

    private String updateCurrentDbInfo(String ipPortInfo) {
        return currentDbInfo = ipPortInfo + ":" + jobconf.getServerId();
    }

    private String getDbIpAddress(InetSocketAddress dbConnectionAddress) {
        if (dbConnectionAddress != null) {
            return dbConnectionAddress.toString();
        }
        return masterInfo.getAddress().toString();
    }

    private boolean isValidCheckSum(byte[] data) {
        return Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x00})
                || Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x01});
    }

    private int calcCheckSum(byte[] data) {
        return Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x01}) ? 1 : 0;
    }

    public State getParseDisptcherStatus() {
        return parseDisptcherStatus;
    }

    public long getPkgIndexId() {
        return oldTimeStampler;
    }

    public void setPkgIndexId(long pkgIndexId) { //TODO: check no usage
        this.pkgIndexId = pkgIndexId;
        this.oldSecStamp = pkgIndexId / this.SECOND_MV_BIT;
//        this.index = (int)(pkgIndexId % this.SECOND_MV_BIT);
        this.oldTimeStampler = pkgIndexId + 10000L;
    }

    public long genIndexOrder(long timeStamp, int transNum) {
        long dataTimeStamp = timeStamp;
        if (timeStamp > this.MAX_SECOND) {
            dataTimeStamp = timeStamp / 1000;
        }
        long dataOrderIndex;
        if (dataTimeStamp > oldSecStamp) {
            this.oldSecStamp = dataTimeStamp;
            dataOrderIndex = dataTimeStamp * this.SECOND_MV_BIT;
            oldTimeStampler = dataOrderIndex;
        } else {
            dataOrderIndex = oldTimeStampler;
        }

        oldTimeStampler += transNum;

        return dataOrderIndex;
    }

    protected LogPosition buildLastPosition(Entry entry) {
        return buildLastPosition(entry, masterInfo.getAddress());
    }

    protected LogPosition buildLastPosition(Entry entry,
            InetSocketAddress address) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(entry.getHeader().getLogfileName());
        position.setPosition(entry.getHeader().getLogfileOffset());
        position.setTimestamp(entry.getHeader().getExecuteTime());
        position.setServerId(entry.getHeader().getServerId());
        LogPosition logPosition = new LogPosition();
        logPosition.setPosition(position);
        LogIdentity identity = new LogIdentity(address, -1L);
        logPosition.setIdentity(identity);
        return logPosition;
    }

    private Long findServerId(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show variables like 'server_id'");
            List<String> fields = packet.getFieldValues();
            if (DBSyncUtils.isCollectionsEmpty(fields)) {
                throw new CanalParseException("command : show variables like 'server_id' has an error! pls check. "
                        + "you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            return Long.valueOf(fields.get(1));
        } catch (IOException e) {
            throw new CanalParseException("command : show variables like 'server_id' has an error!", e);
        }
    }

    protected EntryPosition findStartPosition(MysqlConnection connection) throws IOException {

        EntryPosition startPosition = null;
        if (lastLog != null && bBinlogMiss) {
            bBinlogMiss = false;
            startPosition = lastLog.getPosition();
            //if occur binlog miss, in the same time, the start pos = 4
            //we seek the next binlog file
            if (startPosition.getPosition() == DBSYNC_BINLOG_START_OFFEST) {
                String nextBinlogFileName =
                        DBSyncUtils.getNextBinlogFileName(startPosition.getJournalName());
                EntryPosition endPos = findEndPosition(connection);
                if (nextBinlogFileName.compareToIgnoreCase(endPos.getJournalName()) < 0) {
                    startPosition.setJournalName(nextBinlogFileName);
                } else {
                    startPosition.setJournalName(endPos.getJournalName());
                }
            }

            //occure binlog miss, set the start pos is binlog file head
            //there has the pos error,the dump pos is wrong event head pos,
            //but the binlog file is still in mysql
            //so reset the pos to 4(binlog file head),redump the whole binlog file.
            startPosition.setPosition(DBSYNC_BINLOG_START_OFFEST);
        } else {
            startPosition = findStartPositionInternal(connection);
        }

        if (needTransactionPosition.get()) {
            if (startPosition != null) {
                LOGGER.info("jobName {} prepare to find last position : {}",
                        jobName, startPosition.getJsonObj());
            }
            Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);
            if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
                LOGGER.info("jobName {} find new start Transaction Position , old : {} , new : {}",
                        jobName, startPosition.getPosition(), preTransactionStartPosition);
                startPosition.setPosition(preTransactionStartPosition);
            }
            needTransactionPosition.compareAndSet(true, false);
        }
        return startPosition;
    }

    protected EntryPosition findStartPositionInternal(MysqlConnection mysqlConnection) {
        // can't find success record from history
        if (lastLog == null) {
            EntryPosition entryPosition = null;
            if (masterInfo != null && mysqlConnection.getConnector().getAddress().equals(masterInfo.getAddress())) {
                entryPosition = masterPosition;
            }

            if (entryPosition == null) {
                entryPosition = findEndPosition(mysqlConnection); // consume from the last position
            }

            // whether sub according to timestamp
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // if special binlogName is set, try to find according to timestamp
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                    return findByStartTimeStamp(mysqlConnection, entryPosition.getTimestamp());
                } else {
                    return findEndPosition(mysqlConnection);
                }
            } else {
                if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
                    // if setting binlogName+offset, directly return
                    return entryPosition;
                } else {
                    EntryPosition specificLogFilePosition = null;
                    if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                        // if setting binlogName+timestamp, but not set offest
                        // try to find offset according to timestamp
                        EntryPosition endPosition = findEndPosition(mysqlConnection);
                        if (endPosition != null) {
                            specificLogFilePosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                                    entryPosition.getTimestamp(), endPosition, entryPosition.getJournalName());
                        }
                    }

                    if (specificLogFilePosition == null) {
                        // no position, find from the file head
                        entryPosition.setPosition(DBSYNC_BINLOG_START_OFFEST);
                        return entryPosition;
                    } else {
                        return specificLogFilePosition;
                    }
                }
            }
        } else {
            if (Objects.equals(lastLog.getIdentity().getSourceAddress(),
                    mysqlConnection.getConnector().getAddress())) {
                Long lastServerId = lastLog.getPosition().getServerId();
                if (lastServerId != null && false) {
                    long currentServerId = findServerId(mysqlConnection);
                    if (!lastServerId.equals(currentServerId)) {
                        LOGGER.info("serverId {} not equal with current address {} serverId {}",
                                lastServerId, mysqlConnection.getConnector().getAddress(), currentServerId);
                        return fallbackFindByStartTimestamp(lastLog, mysqlConnection);
                    } else {
                        return lastLog.getPosition();
                    }
                } else {
                    LOGGER.info("{} use position {} ", jobconf.getJobName(), lastLog.getPosition());
                    return lastLog.getPosition();
                }
            } else {
                // for ha, considering backtrace
                LOGGER.info("lastLog address {} not equal with current address {}",
                        lastLog.getIdentity().getSourceAddress(), mysqlConnection.getConnector().getAddress());
                return fallbackFindByStartTimestamp(lastLog, mysqlConnection);
            }
        }
    }

    private EntryPosition fallbackFindByStartTimestamp(LogPosition logPosition, MysqlConnection mysqlConnection) {
        long timestamp = logPosition.getPosition().getTimestamp();
        long newStartTimestamp = timestamp - fallbackIntervalInSeconds * 1000L;
        LOGGER.warn("prepare to find start position by timestamp {}", logPosition.getPosition().getTimestamp());
        return findByStartTimeStamp(mysqlConnection, newStartTimestamp);
    }

    protected Long findTransactionBeginPosition(MysqlConnection mysqlConnection, final EntryPosition entryPosition)
            throws IOException {
        //find position
        final AtomicBoolean reDump = new AtomicBoolean(false);
        mysqlConnection.reconnect();
        mysqlConnection.seek(entryPosition.getJournalName(), entryPosition.getPosition(), new SinkFunction<LogEvent>() {

            private LogPosition lastPosition;

            public boolean sink(LogEvent event) {
                try {
                    LogHeader logHead = event.getHeader();
                    if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                        //skip heartbeat event
                        return true;
                    }

                    Entry entry = parseAndProfilingIfNecessary(event,
                            jobconf);
                    if (entry == null) {
                        reDump.set(true);
                        return false;
                    }

                    // check transaction is Begin or End
                    if (EntryType.TRANSACTIONBEGIN
                            == entry.getEntryType()
                            || EntryType.TRANSACTIONEND
                            == entry.getEntryType()) {
                        lastPosition = buildLastPosition(entry);
                        return false;
                    } else {
                        LOGGER.info("jobName {} set reDump to true", jobconf.getJobName(),
                                entry.getEntryType());
                        reDump.set(true);
                        lastPosition = buildLastPosition(entry);
                        return false;
                    }
                } catch (Exception e) {
                    LOGGER.error("jobName {} parse error", jobconf.getJobName(), e);
                    processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                }
            }
        });
        // for the first record which is not binlog, start to scan from it
        if (reDump.get()) {
            final AtomicLong preTransactionStartPosition = new AtomicLong(0L);
            mysqlConnection.reconnect();
            mysqlConnection.seek(entryPosition.getJournalName(), DBSYNC_BINLOG_START_OFFEST,
                    new SinkFunction<LogEvent>() {

                        private LogPosition lastPosition;

                        public boolean sink(LogEvent event) {
                            try {
                                LogHeader logHead = event.getHeader();
                                if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                                    //skip heartbeat event
                                    return true;
                                }

                                Entry entry = parseAndProfilingIfNecessary(
                                        event, jobconf);
                                if (entry == null) {
                                    return true;
                                }

                                // check whether is Begin transaction
                                // record transaction begin position
                                if (entry.getEntryType()
                                        == EntryType.TRANSACTIONBEGIN
                                        && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                                    preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());
                                }

                                if (entry.getHeader().getLogfileOffset() >= entryPosition.getPosition()) {
                                    LOGGER.info("JobName [{}] find first start position before entry "
                                            + "position {}", jobName, entryPosition.getJsonObj());
                                    return false;// exit
                                }

                                lastPosition = buildLastPosition(entry);
                            } catch (Exception e) {
                                processError(e, lastPosition, entryPosition.getJournalName(),
                                        entryPosition.getPosition());
                                return false;
                            }

                            return running;
                        }
                    });

            if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
                LOGGER.error("JobName [{}] preTransactionEndPosition greater than startPosition, "
                        + "maybe lost data", jobName);
                throw new CanalParseException(
                        "preTransactionStartPosition greater than startPosition, maybe lost data");
            }
            return preTransactionStartPosition.get();
        } else {
            return entryPosition.getPosition();
        }
    }

    // find binlog position according to time
    protected EntryPosition findByStartTimeStamp(MysqlConnection mysqlConnection, Long startTimestamp) {
        EntryPosition endPosition = findEndPosition(mysqlConnection);
        EntryPosition startPosition = findStartPositionCmd(mysqlConnection);
        String maxBinlogFileName = startPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();
        LOGGER.info("startTimeStamp:{}, end condition:{}", startTimestamp, endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();
        boolean shouldBreak = false;
        while (running && !shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection, startTimestamp,
                        endPosition, startSearchBinlogFile);
                if (entryPosition == null) {
                    if (StringUtils.equalsIgnoreCase(minBinlogFileName, startSearchBinlogFile)) {
                        // finded the earliest pos
                        shouldBreak = true;
                        LOGGER.warn("Didn't find the corresponding binlog files from {} to {}", minBinlogFileName,
                                maxBinlogFileName);
                    } else {
                        // continue to find
                        int binlogSeqNum = Integer
                                .parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                        if (binlogSeqNum <= 1) {
                            LOGGER.warn("Didn't find the corresponding binlog files");
                            shouldBreak = true;
                        } else {
                            int nextBinlogSeqNum = binlogSeqNum - 1;
                            String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                                    startSearchBinlogFile.indexOf(".") + 1);
                            String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                            startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                        }
                    }
                } else {
                    LOGGER.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                LOGGER.warn("the binlogfile:{} doesn't exist, to continue to search the next binlogfile , caused by {}",
                        startSearchBinlogFile, ExceptionUtils.getFullStackTrace(e));
                int binlogSeqNum = Integer
                        .parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    LOGGER.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                            startSearchBinlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }
        }

        if (agentConf.getBoolean(DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS,
                DEFAULT_DBSYNC_IS_READ_FROM_FIRST_BINLOG_IF_MISS)) {
            /*
             * return the first binlog position exist in back db
             */
            MonitorLogUtils.printStartPositionWhileMiss(jobName,
                    mysqlConnection.getConnector().getAddress().toString(),
                    (startPosition == null ? "null" : startPosition.getJsonObj().toJSONString()));
            LOGGER.info("start get start position {}", startPosition);
            return startPosition;
        }
        // cann't find
        return null;
    }

    protected EntryPosition findAsPerTimestampInSpecificLogFile(MysqlConnection mysqlConnection,
            final Long startTimestamp, final EntryPosition endPosition, final String searchBinlogFile) {

        final LogPosition logPosition = new LogPosition();
        try {
            mysqlConnection.reconnect();
            // start to scan file
            mysqlConnection.seek(searchBinlogFile, DBSYNC_BINLOG_START_OFFEST, new SinkFunction<LogEvent>() {
                String journalName = null;
                private boolean bFirstEvent = true;
                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        LogHeader logHead = event.getHeader();
                        if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            //skip heartbeat event
                            return true;
                        }
                        if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                            journalName = ((RotateLogEvent) event).getFilename();
                        }

                        Long logfileoffset = event.getHeader().getLogPos();
                        Long logposTimestamp = event.getHeader().getWhen() * 1000;

                        if (logposTimestamp >= startTimestamp) {
                            LOGGER.warn("exit: logposTimestamp:{} >= startTimestamp:{}",
                                    logposTimestamp, startTimestamp);
                            return false;
                        }

                        if (Objects.equals(endPosition.getJournalName(), journalName)
                                && endPosition.getPosition() <= logfileoffset) {
                            LOGGER.warn("exit: journalName:{}, endPosition:{} <= logfilePosition:{}",
                                    journalName, endPosition.getPosition(), logfileoffset);
                            return false;
                        }

                        Entry entry = parseAndProfilingIfNecessary(
                                event, jobconf);
                        if (entry == null) {
                            return true;
                        }

                        String logfilename = entry.getHeader().getLogfileName();
                        long serverId = entry.getHeader().getServerId();

                        // record position of last finished transaction
                        if (EntryType.TRANSACTIONEND.equals(
                                entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            LOGGER.debug("set {} to be pending end position before finding another proper one...",
                                    entryPosition);
                            logPosition.setPosition(entryPosition);
                        } else if (EntryType.TRANSACTIONBEGIN.equals(
                                entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            LOGGER.debug("set {} to be pending start position before finding another proper one...",
                                    entryPosition);
                            logPosition.setPosition(entryPosition);
                        }

                        lastPosition = buildLastPosition(entry);
                    } catch (Throwable e) {
                        processError(e, lastPosition, searchBinlogFile, DBSYNC_BINLOG_START_OFFEST);
                    }

                    return running;
                }
            });

        } catch (IOException e) {
            LOGGER.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e);
        }

        if (logPosition.getPosition() != null) {
            LOGGER.warn("findAsPerTimestampInSpecificLogFile find position:" + logPosition.getPosition());
            return logPosition.getPosition();
        } else {
            return null;
        }
    }

    // search the position of current binlog
    protected EntryPosition findStartPositionCmd(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show binlog events limit 1");
            List<String> fields = packet.getFieldValues();
            if (DBSyncUtils.isCollectionsEmpty(fields)) {
                throw new CanalParseException("command : 'show binlog events limit 1' has an error! "
                        + "pls check. you need (at least one of) the SUPER,REPLICATION CLIENT"
                        + " privilege(s) for this operation");
            }
            return new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
        } catch (IOException e) {
            throw new CanalParseException("command : 'show binlog events limit 1' has an error!", e);
        }

    }

    protected void processError(Throwable e, LogPosition lastPosition, String startBinlogFile, long startPosition) {
        if (lastPosition != null) {
            LOGGER.warn(
                    String.format("ERROR ## parse this event has an error , last position : [%s], exception stack : %s",
                            lastPosition.getPosition(), DBSyncUtils.getExceptionStack(e)),
                    e);
        } else {
            LOGGER.warn(String.format(
                    "ERROR ## parse this event has an error , last position : [%s,%s], exception stack : %s",
                    startBinlogFile, startPosition, DBSyncUtils.getExceptionStack(e)), e);
        }
    }

    protected void flushLastPosition(LogPosition logPosition) {
        this.lastLog = logPosition;
    }

    private class InBoxBoolean {

        boolean bFound = false;
    }

    private class AckLogPositionThread extends Thread {

        public void run() {
            Long lastTimeStamp = Instant.now().toEpochMilli();
            LOGGER.info("AckLogPositionThread started!");
            while (running) {
                try {
                    boolean hasRemoved = false;
                    LogPosition logPosition = ackLogPositionList.poll(1, TimeUnit.SECONDS);
                    if (logPosition != null) {
                        hasRemoved = sendLogPositionCache.remove(logPosition);
                        if (hasRemoved) {
                            semaphore.release();
                        } else {
                            LOGGER.warn("[{}] ack log position is not exist in cache may has "
                                    + "error! Position = {}", jobName, logPosition);
                        }
                    }
                    long currentTimeStamp = Instant.now().toEpochMilli();
                    if (LOGGER.isDebugEnabled()
                            && (currentTimeStamp - lastTimeStamp) > printStatIntervalMs) {
                        LOGGER.debug("[{}], AckLogPositionThread position in cache [{}],"
                                        + " logPosition {}, cache size = {}",
                                jobName, hasRemoved, logPosition, sendLogPositionCache.size());
                        lastTimeStamp = currentTimeStamp;
                    }
                } catch (Throwable e) {
                    if (running) {
                        LOGGER.error("AckLogPositionThread has exception e = ", e);
                    }
                }
            }
        }
    }
}
