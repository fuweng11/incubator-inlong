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

package org.apache.inlong.agent.core.dbsync;

import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.common.protocol.CanalEntry.EntryType;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.DbAddConfigInfo;
import org.apache.inlong.agent.core.dbsync.ha.JobRunNodeInfo;
import org.apache.inlong.agent.core.task.Task;
import org.apache.inlong.agent.except.DBSyncServerException.JobInResetStatus;
import org.apache.inlong.agent.except.DataSourceConfigException;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.metrics.MetricReport;
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
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.state.JobStat.State;
import org.apache.inlong.agent.state.JobStat.TaskStat;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.ErrorCode;
import org.apache.inlong.agent.utils.MonitorLogUtils;
import org.apache.inlong.agent.utils.SnowFlakeManager;
import org.apache.inlong.agent.utils.countmap.UpdaterMap;
import org.apache.inlong.agent.utils.countmap.UpdaterMapFactory;
import org.apache.inlong.common.pojo.agent.dbsync.DbSyncHeartbeat;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.inlong.agent.constant.AgentConstants.*;

/**
 * Refactor from AsyncParseMysqlJob in DBSync
 */
public class DbAgentReadJob implements MetricReport {

    protected static final long SECOND_MV_BIT = 1000000000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DbAgentReadJob.class);
    private static final AtomicReferenceFieldUpdater<DbAgentReadJob, State> STATUS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DbAgentReadJob.class, JobStat.State.class, "status");
    private static final long MAX_SECOND = 9999999999L;

    private volatile int jobParseMaxThreadNum = 20;

    private final AgentConfiguration agentConf;
    private final Object syncObject = new Object();
    private final DBSyncJob job;

    protected DBSyncJobConf dbSyncJobConf;

    protected AtomicBoolean needTransactionPosition = new AtomicBoolean(false);
    protected volatile boolean dispatchRunning = false;
    protected UpdaterMap<Integer, AtomicLong> sendMsgCnt;
    protected volatile MysqlConnection metaConnection;
    protected boolean bBinlogMiss = false;
    protected int fallbackIntervalInSeconds = 10;

    protected volatile long pkgIndexId = 0L;

    protected CanalEventFilter<String> eventFilter;
    protected BinlogParser binlogParser;

    protected volatile boolean running = false;
    protected volatile long heartBeat = 0;
    private boolean isDebug;
    private DbAddConfigInfo mysqlCurrentAddress;
    private String dbJobId;
    private LogPosition lastLog;

    private LogPosition expectedStartPosition = null;

    private LogPosition realStartPosition = null;

    private int reConnectCnt = 0;
    private int doSwitchCnt = 0;

    private RelayLog relayLog;
    private JobStat.State parseDispatcherStatus;
    private TableMetaCache tableMetaCache;
    private AtomicBoolean needReset = new AtomicBoolean(false);
    private CompletableFuture<Void> resetFuture = null;
    private AtomicBoolean isSwitching = new AtomicBoolean(false);
    private AtomicBoolean inSwitchParseDone = new AtomicBoolean(false);

    private ConcurrentHashMap<Integer, BinlogParseThread> parseThreadMap;
    private Thread dispatcherThread = null;
    private Thread dumpThread = null;
    private String currentJobAndDbInfo = "";
    private long slaveId = -1;
    private volatile JobStat.State status;
    private String errorMsg = "";
    private volatile long oldSecStamp = 0L;
    private volatile long oldTimeStampler = 10000L;

    private DbAgentDumpMetric dumpMetricInfo = new DbAgentDumpMetric();
    private boolean jumpPoint = false;

    private SnowFlakeManager snowFlakeManager = new SnowFlakeManager();
    private volatile Integer jobRunningModel = DBSyncJobConf.RUNNING_MODEL_NORMAL;
    private final ReadJobPositionManager readJobPositionManager;

    public DbAgentReadJob(DBSyncJob job) {
        this.job = job;

        /*
         * agent conf
         */
        agentConf = AgentConfiguration.getAgentConf();
        initFromAgentConfig(agentConf);

        /*
         * job conf
         */
        dbSyncJobConf = job.getDBSyncJobConf();
        initFromJobConfig(dbSyncJobConf);

        /*
         * parse thread
         */
        parseThreadMap = new ConcurrentHashMap<>();
        this.readJobPositionManager = new ReadJobPositionManager(dbSyncJobConf);;
    }

    private void initFromAgentConfig(AgentConfiguration agentConf) {
        isDebug = agentConf.getBoolean(DBSYNC_IS_DEBUG_MODE, DEFAULT_DBSYNC_IS_DEBUG_MODE);
        if (isDebug) {
            sendMsgCnt = UpdaterMapFactory.getIntegerUpdater();
        }
        if (agentConf.getBoolean(DBSYNC_IS_NEED_TRANSACTION, DEFAULT_IS_NEED_TRANSACTION)) {
            needTransactionPosition.compareAndSet(false, true);
            LOGGER.info("set need transaction position to true");
        }
        doSwitchCnt = agentConf.getInt(DBSYNC_JOB_DO_SWITCH_CNT, DEFAULT_DBSYNC_JOB_DO_SWITCH_CNT);
        jobParseMaxThreadNum =
                agentConf.getInt(DBSYNC_JOB_MAX_PARSE_THREAD_NUM, DEFAULT_DBSYNC_JOB_MAX_PARSE_THREAD_NUM);
    }

    private void initFromJobConfig(DBSyncJobConf jobconf) {
        dbJobId = jobconf.getDbJobId();
        lastLog = jobconf.getStartPos();
        mysqlCurrentAddress = jobconf.getCurMysqlAddress();
        eventFilter = jobconf.getFilter();
        slaveId = DBSyncUtils.generateSlaveId(dbJobId, jobconf.getDbJobId());
        binlogParser = buildParser();
        relayLog = mkRelayLog();
    }

    public DBSyncJobConf getDbSyncJobConf() {
        return dbSyncJobConf;
    }

    public void start() {
        running = true;
        readJobPositionManager.start();
        setState(State.INIT);
        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_INIT);
        dispatchRunning = true;
        dispatcherThread = mkDispatcher();
        dumpThread = mkDumpThread();
        dumpThread.start();
        dispatcherThread.start();
        setState(State.RUN);
        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_START_RUN);
    }

    public boolean taskIsTaskFinishInit() {
        return job.getTask().isTaskFinishInit();
    }

    public void addMessage(DBSyncMessage message) {
        Task task = job.getTask();
        if (task == null || task.getReader() == null) {
            LOGGER.warn("Fail to get task[{}]!", dbJobId);
            return;
        }
        task.getReader().addMessage(message);
        readJobPositionManager.updateWaitAckCnt(1);
    }

    public boolean isFinished() {
        return !running;
    }

    public State getState() {
        return status;
    }

    private void setState(State state) {
        LOGGER.debug("setState  current status {}, change state {}: ", state, state);
        STATUS_UPDATER.set(DbAgentReadJob.this, state);
        status = state;
    }

    public MysqlConnection buildErosaConnection(DbAddConfigInfo mysqlAddress) {
        String address = mysqlAddress.getDbAddress();
        String ip = address;
        try {
            InetAddress[] addresses = InetAddress.getAllByName(address);
            if (addresses != null && addresses.length > 0) {
                int index = (Math.abs((int) Instant.now().toEpochMilli())) % addresses.length;
                ip = addresses[index].getHostAddress();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        LOGGER.info("dbJobId {} buildErosaConnection url {}, ip = {}", dbJobId, mysqlAddress, ip);
        if (!address.equals(ip)) {
            mysqlAddress.updateRemoteIp(ip);
        }
        MysqlConnection connection = new MysqlConnection(new InetSocketAddress(ip, mysqlAddress.getPort()),
                dbSyncJobConf.getMysqlUserName(), dbSyncJobConf.getMysqlPassWd(), (byte) 33, "information_schema");
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
        /*
         * 停止处理ack
         */
        readJobPositionManager.stop();

        // stop dump 数据
        relayLog.close();
        if (dumpThread != null) {
            dumpThread.interrupt();
            dumpThread = null;
        }

        // stop dispatch
        dispatchRunning = false;
        if (dispatcherThread != null) {
            dispatcherThread.interrupt();
            dispatcherThread = null;
        }

        // stop parse
        if (parseDispatcherStatus != null && parseDispatcherStatus != JobStat.State.STOP) {
            LOGGER.warn("Now wait [{}] in dispatcher thread to stop!", dbJobId);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /*
         * 关闭解析数据
         */
        while (true) {
            boolean isAllClosed = true;
            if (parseThreadMap != null) {
                Set<Map.Entry<Integer, BinlogParseThread>> entriesSet = parseThreadMap.entrySet();
                for (Map.Entry<Integer, BinlogParseThread> entry : entriesSet) {
                    if (!entry.getValue().isEmpty()) {
                        isAllClosed = false;
                        break;
                    }
                }
            }
            if (!isAllClosed) {
                try {
                    LOGGER.warn("Now wait [{}] BinlogParseThread to stop!", dbJobId);
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
                continue;
            }
            break;
        }
        if (parseThreadMap != null) {
            for (BinlogParseThread parser : parseThreadMap.values()) {
                parser.stopParse();
            }
            parseThreadMap.clear();
        }
    }

    public void setHeartBeat(long heartBeat) {
        this.heartBeat = heartBeat;
    }

    protected BinlogParser<LogEvent> buildParser() {
        LogEventConvert convert = new LogEventConvert();
        if (eventFilter != null && eventFilter instanceof CanalEventFilter<?>) {
            LOGGER.info("Job {} set job filter!", this.dbJobId);
            convert.setNameFilter(eventFilter);
        }

        convert.setCharset(dbSyncJobConf.getCharset());
        return convert;
    }

    // TODO: use, updateConnectionCharset
    public void updateConnectionCharset() {
        for (BinlogParseThread parser : parseThreadMap.values()) {
            parser.updateCharSet(dbSyncJobConf.getCharset());
        }
    }

    public DbSyncHeartbeat genHeartBeat(boolean markToStop) {

        DbSyncHeartbeat hbInfo = new DbSyncHeartbeat();
        hbInfo.setInstance(AgentUtils.getLocalIp());
        hbInfo.setServerName(dbSyncJobConf.getDbJobId());
        hbInfo.setTaskIds(dbSyncJobConf.getTaskIdList());
        hbInfo.setCurrentDb(dbSyncJobConf.getCurMysqlIp());
        hbInfo.setUrl(dbSyncJobConf.getCurMysqlUrl());
        hbInfo.setBackupUrl(dbSyncJobConf.getBakMysqlUrl());
        hbInfo.setReportTime(System.currentTimeMillis());

        if (status == JobStat.State.RUN || markToStop) {
            if (markToStop) {
                hbInfo.setAgentStatus("STOPPED");
            } else {
                hbInfo.setAgentStatus(dbSyncJobConf.getStatus().name());
            }
            hbInfo.setDumpIndex(getPkgIndexId());

            LogPosition jobPosition = readJobPositionManager.getSendAndAckedLogPosition();
            if (jobPosition != null) {
                hbInfo.setDumpPosition(jobPosition.genDumpPosition());
            }
            LogPosition newestLogPosition = readJobPositionManager.getNewestLogPositionFromCache();
            if (newestLogPosition != null) {
                hbInfo.setMaxLogPosition(newestLogPosition.genDumpPosition());
            }
            LogPosition oldestLogPosition = readJobPositionManager.getOldestLogPositionFromCache();
            if (oldestLogPosition != null) {
                hbInfo.setOldestLogPosition(oldestLogPosition.genDumpPosition());
            }
            job.sendMetricPositionRecord(newestLogPosition, jobPosition, oldestLogPosition);

        } else {
            LOGGER.error("Detected job :{} is in wrong state :{}", dbSyncJobConf.getDbJobId(), status);
            hbInfo.setErrorMsg(getErrorMsg());
            hbInfo.setAgentStatus(status == null ? "ERROR" : status.name());
            LogPosition jobPosition = readJobPositionManager.getSendAndAckedLogPosition();
            LogPosition newestLogPosition = readJobPositionManager.getNewestLogPositionFromCache();
            LogPosition oldestLogPosition = readJobPositionManager.getOldestLogPositionFromCache();
            job.sendMetricPositionRecord(newestLogPosition, jobPosition, oldestLogPosition);
        }
        return hbInfo;
    }

    protected void preDump(MysqlConnection connection) {
        if (binlogParser != null && binlogParser instanceof LogEventConvert) {
            metaConnection = connection.fork();
            try {
                metaConnection.connect();
                readJobPositionManager.getNewestLogPosition(metaConnection);
                readJobPositionManager.getOldestLogPosition(metaConnection);
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
                BinlogImage image = metaConnection.getBinlogImage();
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
            InetSocketAddress inetSocketAddress = metaConnection.getConnector().getAddress();
            if (dispatcherThread != null && parseThreadMap.size() == 0) {
                readJobPositionManager.clearCache();
                LOGGER.info("{} build binlog parser!", this.dbJobId);
                for (int i = 0; i < jobParseMaxThreadNum; i++) {
                    BinlogParseThread parser = new BinlogParseThread("parser-"
                            + i + "-" + this.dbJobId, inetSocketAddress, this, snowFlakeManager);
                    parser.start();
                    parseThreadMap.put(i, parser);
                }
            } else {
                Set<Map.Entry<Integer, BinlogParseThread>> parseThreadSet = parseThreadMap.entrySet();
                for (Map.Entry<Integer, BinlogParseThread> entry : parseThreadSet) {
                    entry.getValue().updateCurrentDbAddress(inetSocketAddress);
                }
            }
            readJobPositionManager.updateMetaConnection(metaConnection);
            readJobPositionManager.setNeedUpdateLogPosition(true);
            LOGGER.info("TableMetaCache use MysqlConnection port : {} : {}", dbJobId, metaConnection.getLocalPort());
        }
    }

    private RelayLog mkRelayLog() {
        String relogRoot = agentConf.get(DBSYNC_RELAY_LOG_ROOT, DEFAULT_DBSYNC_RELAY_LOG_ROOT).trim();
        String relayLogWay = agentConf.get(DBSYNC_RELAY_LOG_WAY, DEFAULT_RELAY_LOG_WAY);
        RelayLog log = null;
        if (relayLogWay != null && relayLogWay.trim().endsWith("memory")) {
            int memSize = agentConf.getInt(DBSYNC_RELAY_LOG_MEM_SZIE, DEFAULT_RELAY_LOG_MEM_SZIE);
            log = new MemRelayLogImpl(memSize * 1024 * 1024L);
        } else {
            String jobRelyLogPath = null;
            String jobPrefix = mysqlCurrentAddress.getDbAddress().replace('.', '-')
                    + "-" + mysqlCurrentAddress.getPort();
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
            LOGGER.info("{} dispatcher thread begin work!", dbJobId);

            boolean bInTransaction = false;
            parseDispatcherStatus = State.RUN;
            int index = 0;
            String logfileName = null;
            long lastEventStp = 0L;

            ArrayList<Object> bufferList = new ArrayList<>();
            while (dispatchRunning) {
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
                    if (dispatchRunning) {
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
                        // won't send event, update gtid directly, or else update gtid when ack
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
                    if (eventTmStp > 0) {
                        lastEventStp = eventTmStp;
                    }

                    Entry entry = null;
                    if (event.getHeader().getType() == LogEvent.ROTATE_EVENT) {
                        logfileName = ((RotateLogEvent) event).getFilename();
                    }
                    if (event.getHeader().getType() == LogEvent.XID_EVENT
                            || event.getHeader().getType() == LogEvent.QUERY_EVENT) {
                        entry = parseAndProfilingIfNecessary(event, dbSyncJobConf);
                        if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                            bufferList.add(entry);
                            bInTransaction = true;
                        } else {
                            bufferList.add(entry);
                            index = dispatchEventToParseThread(index, eventTmStp,
                                    bufferList, logfileName, 10);
                            bufferList = new ArrayList<>();
                        }
                        if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                            bInTransaction = false;
                        }
                    } else if (bInTransaction) {
                        bufferList.add(event);
                        // if bufferList size to large need lock a
                        // process
                        if (bufferList.size() > 20) {
                            index = dispatchEventToParseThread(index, eventTmStp,
                                    bufferList, logfileName, 100);
                            bufferList = new ArrayList<>();
                        }
                    } else if (event.getHeader().getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                        bufferList.add(event);
                        index = dispatchEventToParseThread(index, lastEventStp,
                                bufferList, logfileName, 10);
                        bufferList = new ArrayList<>();
                    } else {
                        entry = parseAndProfilingIfNecessary(event, dbSyncJobConf);
                        if (entry != null) {
                            bufferList.add(entry);
                            index = dispatchEventToParseThread(index, lastEventStp,
                                    bufferList, logfileName, 10);
                            bufferList = new ArrayList<>();
                        }
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    LOGGER.error("{} async dispatcher Error : {}", dbJobId, DBSyncUtils.getExceptionStack(e));
                }
            }
            LOGGER.info("Job : " + dbJobId + " dispatcher Thread stopped!");

            /*
             * in case that dump thread won't be properly stopped
             */
            inSwitchParseDone.compareAndSet(false, true);

            parseDispatcherStatus = State.STOP;
        });

        dispatcher.setName(dbJobId + "-dispatcher");
        dispatcher.setUncaughtExceptionHandler((t, e) -> {
            setState(State.STOP);
            MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
                    ErrorCode.OP_EXCEPTION_DISPATCH_THREAD_EXIT_UNCAUGHT);
            setErrorMsg(e.getMessage());
            LOGGER.error("{} dispatcher Thread has an uncaught error {}",
                    dbJobId, DBSyncUtils.getExceptionStack(e));
        });

        return dispatcher;
    }

    private int dispatchEventToParseThread(int index, long eventTmStp,
            ArrayList<Object> bufferList, String currentLogFile,
            long sleepTimeMs) {
        if (bufferList == null || bufferList.size() == 0) {
            return index;
        }
        BinlogParseThread parser = null;
        int tmpIndex = index;
        do {
            /*
             * 当reset 或switch 的时候 parseThreadList、 relayLog会重新创建，
             */
            parser = parseThreadMap.get(index);
            int threadSize = parseThreadMap.size();
            if (threadSize > 0) {
                index = (index + 1) % threadSize;
            }
            if (index == tmpIndex) {
                DBSyncUtils.sleep(sleepTimeMs);
            }
        } while (parser != null && parser.bParseQueueIsFull() && dispatchRunning);

        pkgIndexId = genIndexOrder(eventTmStp, bufferList.size() + 1);
        /*
         * 过来的消息只有在resetting 和switch的时候 parser 可能为null，这个时候链接或重建立 ，这里的消息不分发也不会导致丢
         */
        if (parser != null) {
            parser.putEvents(new PkgEvent(bufferList, pkgIndexId, eventTmStp, currentLogFile));
            return index;
        }
        return index;
    }

    public ReadJobPositionManager getJobPositionManager() {
        return readJobPositionManager;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    protected Entry parseAndProfilingIfNecessary(LogEvent bod, DBSyncJobConf jobCof) throws Exception {
        Entry event = binlogParser.parse(bod, jobCof);
        return event;
    }

    public BinlogParser getBinlogParser() {
        return binlogParser;
    }

    public String getDbJobId() {
        return dbJobId;
    }

    private void updateLogPosition() {
        synchronized (syncObject) {
            LogPosition tmpPos = readJobPositionManager.getSendAndAckedLogPosition();
            if (tmpPos != null) {
                lastLog = new LogPosition(tmpPos);
                LOGGER.info("Job {} HA reset pos to {}!",
                        dbJobId, tmpPos.getJsonObj().toJSONString());
            }
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
                        updateLogPosition();
                        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_RESET);
                        if (doReset()) {
                            LOGGER.info("Job {} reset to master ok, new reBuild Relay-log!", dbJobId);
                            relayLog.close();
                            relayLog = mkRelayLog();
                            bNeedClearParseThread = true;
                        } else {
                            LOGGER.info("Job {} reset to master failed", dbJobId);
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                                MonitorLogUtils.JOB_STAT_RESET_FINISHED, "",
                                "mysql current address:" + mysqlCurrentAddress);
                        needReset.set(false);
                        reConnectCnt = 0;
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
                        updateLogPosition();
                        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                                MonitorLogUtils.JOB_STAT_SWITCH);
                        if (doSwitch()) {
                            LOGGER.info("Job {} switch ok, new reBuild Relay-log!", dbJobId);
                            relayLog.close();
                            relayLog = mkRelayLog();
                        }
                        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                                MonitorLogUtils.JOB_STAT_SWITCH_FINISHED, "",
                                "mysql current address:" + mysqlCurrentAddress);
                    }

                    if (bNeedClearParseThread) {
                        bNeedClearParseThread = false;
                        if (parseThreadMap != null && parseThreadMap.size() > 0) {
                            for (BinlogParseThread parser : parseThreadMap.values()) {
                                parser.stopParse();
                            }
                            parseThreadMap.clear();
                            LOGGER.info("Job {} clear binlog parser!", dbJobId);
                        }
                    }

                    // start execute replication
                    // 1. construct Erosa conn
                    erosaConnection = buildErosaConnection(mysqlCurrentAddress);
                    erosaConnection.connect();

                    // whether masterAddr changes after last exception
                    String remoteIp = erosaConnection.getConnector().getRemoteAddress();
                    dbConnectionAddress = erosaConnection.getConnector().getAddress();
                    if (dbConnectionAddress != null) {
                        updateCurrentJobAndDbInfo(dbConnectionAddress.toString());
                    }
                    LOGGER.info("Dumper use MysqlConnection dbJobId/mysqlAddress/remoteIp/port : "
                            + "{}/{}/{}/{}/{}", dbJobId, mysqlCurrentAddress.getDbAddress(),
                            mysqlCurrentAddress.getRealRemoteIp(), remoteIp, erosaConnection.getLocalPort());
                    // 3. get last position info
                    MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                            MonitorLogUtils.JOB_STAT_GET_POSITION, "",
                            (lastLog == null ? "" : lastLog.getJsonObj().toString()));
                    if (expectedStartPosition == null && lastLog != null) {
                        expectedStartPosition = new LogPosition(lastLog);
                    }
                    final EntryPosition startPosition = findStartPosition(erosaConnection);
                    if (startPosition == null) {
                        LOGGER.error("{} can't find start position", dbJobId);
                        throw new CanalParseException("can't find start position");
                    }
                    MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                            MonitorLogUtils.JOB_STAT_GET_POSITION_FINISHED, "",
                            startPosition.getJsonObj().toJSONString());
                    LOGGER.info("job {}, find start position : {}", dbJobId, startPosition);
                    // reconnect
                    erosaConnection.reconnect();
                    LOGGER.info("Dumper use MysqlConnection reconnect dbJobId/mysqlAddress/remoteIp/port : "
                            + "{}/{}/{}/{}", dbJobId, mysqlCurrentAddress, remoteIp, erosaConnection.getLocalPort());

                    final LogPosition lastPosition = new LogPosition();
                    lastPosition.setPosition(startPosition);
                    lastPosition.setIdentity(new LogIdentity(dbConnectionAddress, -1L));
                    realStartPosition = new LogPosition(lastPosition);
                    if (lastLog == null) {
                        lastLog = new LogPosition(lastPosition);
                    }

                    @SuppressWarnings("rawtypes")
                    final SinkFunction sinkHandler = (SinkFunction<LogEvent>) event -> {
                        LogHeader logHead = event.getHeader();
                        if (expectedStartPosition != null) {
                            if (realStartPosition != null && realStartPosition.getPosition() != null) {
                                if (logHead.getWhen() > 0) {
                                    realStartPosition.getPosition().setTimestamp(logHead.getWhen() * 1000);
                                }
                                String fileName = realStartPosition.getPosition().getJournalName();
                                if (StringUtils.isNotEmpty(fileName)) {
                                    dumpMetricInfo.setBinlogFile(fileName);
                                }
                            }
                            MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(),
                                    MonitorLogUtils.JOB_STAT_FETCH_DATA, "",
                                    "jumpPoint=" + jumpPoint
                                            + ",expectedStartPosition="
                                            + expectedStartPosition.toMinString()
                                            + ",realStartPosition="
                                            + realStartPosition.toMinString());
                            expectedStartPosition = null;
                            jumpPoint = false;
                        }
                        dumpMetricInfo.addDumpSize(event.getEventLen());
                        dumpMetricInfo.setWhen(logHead.getWhen() * 1000);
                        dumpMetricInfo.setPosition(event.getLogPos());
                        dumpMetricInfo.setLastEventType(logHead.getType());
                        if (needReset.get()) {
                            // break the loop
                            return false;
                        }
                        if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            // skip heartbeat event
                            return running;
                        }
                        if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                            // skip fake rotate event
                            String newJournalName = ((RotateLogEvent) event).getFilename();
                            dumpMetricInfo.setBinlogFile(newJournalName);
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
                    MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_DUMP_RUN);
                    // 2. predump operation
                    preDump(erosaConnection);
                    erosaConnection.seekAndCopyData(startPosition.getJournalName(), startPosition.getPosition(),
                            sinkHandler, relayLog);
                } catch (RelayLogPosErrorException re) {
                    setState(State.ERROR);
                    MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                            ErrorCode.DB_EXCEPTION_RELAY_LOG_POSITION);
                    setErrorMsg(re.getMessage());

                    LOGGER.error("DbJobId [{}] relay-log file magic error : ", dbJobId, re);
                    LOGGER.error("DbJobId [{}] relay-log use RedumpPosition is : {} ",
                            dbJobId, lastLog);
                } catch (TableIdNotFoundException e) {
                    setState(State.ERROR);
                    MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                            ErrorCode.DB_EXCEPTION_TABLE_ID_NOT_FOUND);
                    setErrorMsg(e.getMessage());

                    needTransactionPosition.compareAndSet(false, true);
                    LOGGER.error("DbJobId [{}] dump address {} has an error, retrying. caused by",
                            dbJobId, getCurrentDbIpAddress(), e);
                } catch (BinlogMissException e) {
                    setState(State.ERROR);
                    setErrorMsg(e.getMessage());
                    LOGGER.error("DbJobId [{}] Dump {} Error : ",
                            dbJobId, getCurrentDbIpAddress(), e);
                    LOGGER.error("DbJobId [{}] Flush old position log : {}",
                            dbJobId, lastLog.getJsonObj().toJSONString());
                    // lastLog = null;
                    bBinlogMiss = true;
                    jumpPoint = true;
                } catch (Throwable e) {
                    if (!running) {
                        if (!(e instanceof java.nio.channels.ClosedByInterruptException
                                || e.getCause() instanceof java.nio.channels.ClosedByInterruptException)) {
                            throw new CanalParseException(String.format("dump address %s has an error, retrying. ",
                                    getCurrentDbIpAddress()), e);
                        }
                    } else {
                        setState(State.ERROR);
                        MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                                ErrorCode.DB_EXCEPTION_OTHER);
                        setErrorMsg(e.getMessage());
                        if (running || LOGGER.isDebugEnabled()) {
                            LOGGER.error("DbJobId [{}] dump address {} has an error, retrying. caused by ",
                                    dbJobId, getCurrentDbIpAddress(), e);
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
                                            getCurrentDbIpAddress(), reConnectCnt),
                                    e1);
                        } else {
                            LOGGER.error("disconnect address {} has an error, retrying {}., "
                                    + "caused by ",
                                    getCurrentDbIpAddress(), reConnectCnt, e1);
                        }
                    }

                    DBSyncUtils.sleep(reConnectCnt * 1000L);
                }
            }

            LOGGER.info("Job : " + dbJobId + " dump Thread stopped!");
            setState(State.STOP);
            MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
                    ErrorCode.OP_EXCEPTION_DUMPER_THREAD_EXIT);
        });
        dumperThread.setName(dbJobId + "-dumper");
        dumperThread.setPriority(Thread.MAX_PRIORITY);
        dumperThread.setUncaughtExceptionHandler((t, e) -> {
            setState(State.STOP);
            setErrorMsg(e.getMessage());
            MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_STOP,
                    ErrorCode.OP_EXCEPTION_DUMPER_THREAD_EXIT_UNCAUGHT);
            LOGGER.error("{} dump Thread has an uncaught error ", dbJobId, e);
        });

        return dumperThread;
    }

    protected void afterDump(MysqlConnection connection) {

        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }
        readJobPositionManager.setNeedUpdateLogPosition(false);
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
                metaConnection = null;
            } catch (IOException e) {
                LOGGER.error("ERROR # disconnect for address:{}", metaConnection.getConnector().getAddress(), e);
            }
        }
    }

    /**
     * open ha swtich
     *
     * @throws IOException
     */
    private boolean doSwitch() throws IOException {
        this.dbSyncJobConf.doSwitch();
        DbAddConfigInfo mysqlAddr = dbSyncJobConf.getNextMysqlAddr();
        String nextAddr = mysqlAddr.getDbAddress();
        int nextPort = mysqlAddr.getPort();
        if (StringUtils.isEmpty(nextAddr) || -1 == nextPort) {
            LOGGER.warn("DbJobId can't do HA switch , next database : {}:{}", nextAddr, nextPort);
            return false;
        }
        LOGGER.info("DbJobId {} HA switch next database : {}:{}, last pos : {}",
                this.dbJobId, nextAddr, nextPort, this.lastLog);
        doSwitchFinal(mysqlAddr);
        return true;
    }

    private void doSwitchFinal(DbAddConfigInfo mysqlAddr) {
        this.mysqlCurrentAddress = mysqlAddr;

        this.dbJobId = dbSyncJobConf.getDbJobId();
        this.dbSyncJobConf.setStatus(TaskStat.SWITCHED);
    }

    /**
     * reset collect position
     *
     * @return reset true if reset succeed or false if reset failed
     * @throws IOException
     */
    private boolean doReset() throws IOException {
        DbAddConfigInfo masterAddr = dbSyncJobConf.getMstMysqlAddr();
        if (masterAddr == null) {
            LOGGER.warn("DbJobId [{}] reset masterAddr is null!", dbJobId);
            resetFuture.complete(null);
            return false;
        }
        int dbPort = masterAddr.getPort();
        String dbAddr = masterAddr.getDbAddress();
        if (StringUtils.isBlank(dbAddr) || -1 == dbPort) {
            LOGGER.warn("{} can't reset, invalid database: {}:{}", this.dbJobId, dbAddr, dbPort);
            dbSyncJobConf.setStatus(TaskStat.RESET);
            resetFuture.completeExceptionally(new DataSourceConfigException(
                    "invalid config, database: " + dbAddr + ":" + dbPort));
            return false;
        }
        if (Objects.equals(dbAddr, mysqlCurrentAddress.getDbAddress())
                && Objects.equals(dbPort, mysqlCurrentAddress.getPort())) {
            LOGGER.info("{}:{} is being dumping, no need to reset", dbAddr, dbPort);
            resetFuture.complete(null);
            return true;
        }
        LOGGER.info("begin switch to master database : {}:{}", dbAddr, dbPort);
        LOGGER.info("{} switch to master, last pos: {}", this.dbJobId, this.lastLog);
        doResetFinal(masterAddr);
        resetFuture.complete(null);
        return true;
    }

    private void doResetFinal(DbAddConfigInfo masterAddr) {
        this.mysqlCurrentAddress = masterAddr;

        this.dbSyncJobConf.doReset();
        this.dbJobId = dbSyncJobConf.getDbJobId();
    }

    public void updateRunningNodeInfo(JobRunNodeInfo jobRunNodeInfo) {
        int oldParseThreadNum = jobParseMaxThreadNum;
        if (jobRunNodeInfo != null && jobRunNodeInfo.getParseThreadNum() > 0
                && (jobRunNodeInfo.getParseThreadNum() > jobParseMaxThreadNum)) {
            LOGGER.info("DbJobId = {} updateRunningNodeInfo parseThreadNum from = {}, to = {}",
                    dbJobId, jobParseMaxThreadNum, jobRunNodeInfo.getParseThreadNum());
            jobParseMaxThreadNum = jobRunNodeInfo.getParseThreadNum();
        }
        if (jobRunNodeInfo.getRunningModel() != null
                && (jobRunningModel != jobRunNodeInfo.getRunningModel())) {
            LOGGER.info("DbJobId = {} updateRunningNodeInfo jobRunningModel from = {}, to = {}",
                    dbJobId, jobRunningModel, jobRunNodeInfo.getRunningModel());
            jobRunningModel = jobRunNodeInfo.getRunningModel();
        }
        if (parseThreadMap != null && parseThreadMap.size() == oldParseThreadNum) {
            if (jobParseMaxThreadNum > oldParseThreadNum) {
                InetSocketAddress inetSocketAddress = metaConnection.getConnector().getAddress();
                for (int i = 0; i < (jobParseMaxThreadNum - oldParseThreadNum); i++) {
                    String threadName = "parser-" + (i + oldParseThreadNum) + "-" + this.dbJobId;
                    LOGGER.info("DbJobId = {} Update parse thread {}", dbJobId, threadName);
                    BinlogParseThread parser = new BinlogParseThread(threadName, inetSocketAddress,
                            this, snowFlakeManager);
                    parser.start();
                    parseThreadMap.put(i + oldParseThreadNum, parser);
                }
            }
        }
    }

    public CompletableFuture<Void> resetRead() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (!needReset.compareAndSet(false, true)) {
            future.completeExceptionally(new JobInResetStatus("job " + dbJobId + " is being resetting"));
        } else {
            resetFuture = future;
        }
        return future;
    }

    public String getCurrentJobAndDbInfo() {
        if (StringUtils.isEmpty(currentJobAndDbInfo)) {
            currentJobAndDbInfo = getCurrentDbIpAddress() + ":" + dbSyncJobConf.getDbJobId();
        }
        return currentJobAndDbInfo;
    }

    public String updateCurrentJobAndDbInfo(String ipPortInfo) {
        return currentJobAndDbInfo = ipPortInfo + ":" + dbSyncJobConf.getDbJobId();
    }

    public String getDumpMetricInfo() {
        return dumpMetricInfo.getAndResetMetricInfo() + "|relayLog-"
                + (relayLog == null ? "" : relayLog.report());
    }

    public String getCurrentDbIpAddress() {
        if (mysqlCurrentAddress != null) {
            return mysqlCurrentAddress.toString();
        }
        return "";
    }

    public String getCurrentDbIpPortStr() {
        if (mysqlCurrentAddress != null) {
            return mysqlCurrentAddress.getIpPortStr();
        }
        return "";
    }

    private boolean isValidCheckSum(byte[] data) {
        return Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x00})
                || Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x01});
    }

    private int calcCheckSum(byte[] data) {
        return Arrays.equals(data, new byte[]{0x00, 0x00, 0x00, 0x01}) ? 1 : 0;
    }

    public State getParseDispatcherStatus() {
        return parseDispatcherStatus;
    }

    public long getPkgIndexId() {
        return oldTimeStampler;
    }

    public Integer getJobRunningModel() {
        return jobRunningModel;
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

    protected LogPosition buildLastPosition(Entry entry, InetSocketAddress address) {
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
            String lostPosition = startPosition.getJsonObj().toString();
            // if occur binlog miss, in the same time, the start pos = 4
            // we seek the next binlog file
            if (startPosition.getPosition() == DBSYNC_BINLOG_START_OFFEST) {
                String nextBinlogFileName =
                        DBSyncUtils.getNextBinlogFileName(startPosition.getJournalName());
                EntryPosition endPos = readJobPositionManager.findEndPosition(connection, true);
                if (nextBinlogFileName.compareToIgnoreCase(endPos.getJournalName()) < 0) {
                    startPosition.setJournalName(nextBinlogFileName);
                } else {
                    startPosition.setJournalName(endPos.getJournalName());
                }
                String nextRetryPosition = startPosition.getJsonObj().toString();
                MonitorLogUtils.printJobStat(this.getCurrentJobAndDbInfo(), MonitorLogUtils.JOB_STAT_ERROR,
                        ErrorCode.DB_EXCEPTION_BINLOG_MISS, "lostPosition:"
                                + lostPosition + ",nextRetryPosition:" + nextRetryPosition);
            }

            // occure binlog miss, set the start pos is binlog file head
            // there has the pos error,the dump pos is wrong event head pos,
            // but the binlog file is still in mysql
            // so reset the pos to 4(binlog file head),redump the whole binlog file.
            startPosition.setPosition(DBSYNC_BINLOG_START_OFFEST);
        } else {
            startPosition = findStartPositionInternal(connection);
        }

        if (needTransactionPosition.get()) {
            if (startPosition != null) {
                LOGGER.info("DbJobId {} prepare to find last position : {}",
                        dbJobId, startPosition.getJsonObj());
            }
            Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);
            if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
                LOGGER.info("DbJobId {} find new start Transaction Position , old : {} , new : {}",
                        dbJobId, startPosition.getPosition(), preTransactionStartPosition);
                startPosition.setPosition(preTransactionStartPosition);
            }
            needTransactionPosition.compareAndSet(true, false);
        }
        return startPosition;
    }

    protected EntryPosition findStartPositionInternal(MysqlConnection mysqlConnection) {
        // can't find success record from history
        if (lastLog == null) {
            // consume from the last position
            EntryPosition entryPosition = readJobPositionManager.findEndPosition(mysqlConnection, true);
            // whether sub according to timestamp
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // if special binlogName is set, try to find according to timestamp
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                    return findByStartTimeStamp(mysqlConnection, entryPosition.getTimestamp());
                } else {
                    return readJobPositionManager.findEndPosition(mysqlConnection, true);
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
                        EntryPosition endPosition = readJobPositionManager.findEndPosition(mysqlConnection, true);
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
                    LOGGER.info("{} use position {} ", dbSyncJobConf.getDbJobId(), lastLog.getPosition());
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
        // find position
        final AtomicBoolean reDump = new AtomicBoolean(false);
        mysqlConnection.reconnect();
        InetSocketAddress address = mysqlConnection.getConnector().getAddress();
        mysqlConnection.seek(entryPosition.getJournalName(), entryPosition.getPosition(), new SinkFunction<LogEvent>() {

            private LogPosition lastPosition;

            public boolean sink(LogEvent event) {
                try {
                    LogHeader logHead = event.getHeader();
                    if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                        // skip heartbeat event
                        return true;
                    }

                    Entry entry = parseAndProfilingIfNecessary(event,
                            dbSyncJobConf);
                    if (entry == null) {
                        reDump.set(true);
                        return false;
                    }

                    // check transaction is Begin or End
                    if (EntryType.TRANSACTIONBEGIN == entry.getEntryType()
                            || EntryType.TRANSACTIONEND == entry.getEntryType()) {
                        lastPosition = buildLastPosition(entry, address);
                        return false;
                    } else {
                        LOGGER.info("DbJobId {} set reDump to true", dbSyncJobConf.getDbJobId(),
                                entry.getEntryType());
                        reDump.set(true);
                        lastPosition = buildLastPosition(entry, address);
                        return false;
                    }
                } catch (Exception e) {
                    LOGGER.error("DbJobId {} parse error", dbSyncJobConf.getDbJobId(), e);
                    processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                }
            }
        });
        // for the first record which is not binlog, start to scan from it
        if (reDump.get()) {
            final AtomicLong preTransactionStartPosition = new AtomicLong(DBSYNC_BINLOG_START_OFFEST);
            mysqlConnection.reconnect();
            mysqlConnection.seek(entryPosition.getJournalName(), DBSYNC_BINLOG_START_OFFEST,
                    new SinkFunction<LogEvent>() {

                        private LogPosition lastPosition;

                        public boolean sink(LogEvent event) {
                            try {
                                LogHeader logHead = event.getHeader();
                                if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                                    // skip heartbeat event
                                    return true;
                                }

                                if (logHead.getType() == LogEvent.ROTATE_EVENT) {
                                    String binlogFileName = ((RotateLogEvent) event).getFilename();
                                    // if rotated to the next binlog,break
                                    if (!entryPosition.getJournalName().equals(binlogFileName)) {
                                        return false;
                                    }
                                }

                                Entry entry = parseAndProfilingIfNecessary(event, dbSyncJobConf);
                                if (entry == null) {
                                    return true;
                                }

                                // check whether is Begin transaction
                                // record transaction begin position
                                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                                        && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                                    preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());
                                }

                                if (logHead.getLogPos() >= entryPosition.getPosition()) {
                                    LOGGER.info("DbJobId [{}] find first start position before entry "
                                            + "position {}", dbJobId, entryPosition.getJsonObj());
                                    return false;// exit
                                }

                                lastPosition = buildLastPosition(entry, address);
                            } catch (Exception e) {
                                processError(e, lastPosition, entryPosition.getJournalName(),
                                        entryPosition.getPosition());
                                return false;
                            }

                            return running;
                        }
                    });

            if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
                LOGGER.error("DbJobId [{}] preTransactionEndPosition greater than startPosition, "
                        + "maybe lost data", dbJobId);
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
        EntryPosition endPosition = readJobPositionManager.findEndPosition(mysqlConnection, true);
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
            MonitorLogUtils.printStartPositionWhileMiss(dbJobId,
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

            InetSocketAddress address = mysqlConnection.getConnector().getAddress();
            // start to scan file
            mysqlConnection.seek(searchBinlogFile, DBSYNC_BINLOG_START_OFFEST, new SinkFunction<LogEvent>() {

                String journalName = null;
                private LogPosition lastPosition;
                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        LogHeader logHead = event.getHeader();
                        if (logHead.getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            // skip heartbeat event
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
                                event, dbSyncJobConf);
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

                        lastPosition = buildLastPosition(entry, address);
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
            synchronized (mysqlConnection) {
                ResultSetPacket packet = mysqlConnection.query("show binlog events limit 1");
                List<String> fields = packet.getFieldValues();
                if (DBSyncUtils.isCollectionsEmpty(fields)) {
                    throw new CanalParseException("command : 'show binlog events limit 1' has an error! "
                            + "pls check. you need (at least one of) the SUPER,REPLICATION CLIENT"
                            + " privilege(s) for this operation");
                }
                return new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            }
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

    public MysqlConnection getMeteConnection() {
        return metaConnection;
    }

    @Override
    public String report() {
        return getDumpMetricInfo();
    }

}
