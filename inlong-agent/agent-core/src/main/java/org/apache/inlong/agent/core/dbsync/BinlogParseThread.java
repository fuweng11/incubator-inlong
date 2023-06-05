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
import org.apache.inlong.agent.common.protocol.CanalEntry.RowChange;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.EventType;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.RowData;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.metrics.MetricReport;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.HeartbeatLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.LogEventConvert;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.connector.exception.TableIdNotFoundException;
import org.apache.inlong.agent.mysql.connector.exception.TableMapEventMissException;
import org.apache.inlong.agent.mysql.protocol.position.EntryPosition;
import org.apache.inlong.agent.mysql.protocol.position.LogIdentity;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DBSyncUtils;
import org.apache.inlong.agent.utils.MonitorLogUtils;
import org.apache.inlong.agent.utils.SnowFlakeManager;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_NEED_SKIP_DELETE_DATA;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_NEED_SKIP_DELETE_DATA;

public class BinlogParseThread extends Thread implements MetricReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogParseThread.class);
    private final String parserJobName;
    private final LinkedBlockingQueue<PkgEvent> queue;
    private SnowFlakeManager snowFlakeManager;
    private final ReadJobPositionManager readJobPositionManager;
    @SuppressWarnings("rawtypes")
    private volatile boolean bParseRunning = false;
    private JobStat.State parseStatus;
    private InetSocketAddress curDbAddress;
    private InetSocketAddress newDbAddress;
    private String lastBinlogFileName;
    private final DbAgentReadJob dbSyncReadJob;
    private String jobName;
    private char ipSep = '@';
    private char sep = ',';

    public BinlogParseThread(String parserName, InetSocketAddress curDbAddress,
            DbAgentReadJob dbSyncReadJob, SnowFlakeManager snowFlakeManager) {
        this.parserJobName = parserName;
        this.curDbAddress = curDbAddress;
        queue = new LinkedBlockingQueue<>();
        this.dbSyncReadJob = dbSyncReadJob;
        this.jobName = dbSyncReadJob.getJobName();
        this.snowFlakeManager = snowFlakeManager;
        super.setUncaughtExceptionHandler((t, e) -> {
            parseStatus = JobStat.State.STOP;
            LOGGER.error("{} Parse Thread has an uncaught error {}, \n {}",
                    jobName, parserJobName, DBSyncUtils.getExceptionStack(e));
        });
        parseStatus = JobStat.State.INIT;
        readJobPositionManager = dbSyncReadJob.getJobPositionManager();
    }

    public void start() {
        super.setName(parserJobName);
        bParseRunning = true;
        super.start();
    }

    @SuppressWarnings("unchecked")
    public void run() {
        PkgEvent pkgEvent = null;
        ArrayList<Object> eventList = null;
        parseStatus = JobStat.State.RUN;
        long parseMsgId = 0;
        LOGGER.info("{}, {} begin running!", jobName, this.parserJobName);
        while (bParseRunning || !queue.isEmpty()) {
            try {
                pkgEvent = queue.poll(1, TimeUnit.SECONDS);

                if (pkgEvent == null) {
                    if (!bParseRunning && dbSyncReadJob.getParseDispatcherStatus() == JobStat.State.STOP
                            && queue.isEmpty()) {
                        LOGGER.error("{}, {} stopped occure trans end event missing!!!",
                                jobName, parserJobName);
                    }
                    continue;
                }
                eventList = pkgEvent.getEventLists();
                parseMsgId = pkgEvent.getIndex();

                ArrayList<Entry> entryList = new ArrayList<>();
                LogPosition firstEventPos = null;
                for (Object eventObj : eventList) {
                    dbSyncReadJob.setHeartBeat(System.currentTimeMillis());
                    Entry entry = null;
                    // Query -> Rows_query -> Table_map -> Update_rows -> Xid
                    if (!bParseRunning) {
                        break;
                    }
                    try {
                        int retryCnt = 0;
                        if (eventObj instanceof LogEvent) {
                            LogEvent event = (LogEvent) eventObj;
                            handleHeartbeatEvent(event, pkgEvent.getTimeStamp(), parseMsgId);
                            while (true) {
                                try {
                                    if (retryCnt > 10 || !bParseRunning) {
                                        LOGGER.error("{}, {} retry 10, now skip an event, pos {} !",
                                                jobName, parserJobName, event.getLogPos());
                                        MonitorLogUtils.printEventDiscard(dbSyncReadJob.getCurrentJobAndDbInfo(),
                                                MonitorLogUtils.EVENT_DISCARD_EXCEED_RETRY_CNT, "");
                                        break;
                                    }
                                    entry = dbSyncReadJob.getBinlogParser().parse(event, dbSyncReadJob.dbSyncJobConf);

                                } catch (TableIdNotFoundException | TableMapEventMissException tie) {
                                    // need redump, refind the beginning of transaction and parse
                                    LOGGER.error("async parse binlog error: ", tie);
                                    MonitorLogUtils.printEventDiscard(dbSyncReadJob.getCurrentJobAndDbInfo(),
                                            MonitorLogUtils.EVENT_DISCARD_TABLE_ID_NOT_FOUND, "");
                                    throw tie;
                                } catch (CanalParseException te) {
                                    LOGGER.error("async parse binlog error: ", te);
                                    LOGGER.error("{}, {} parse binlog event error ", jobName, parserJobName, te);
                                    retryCnt++;
                                    DBSyncUtils.sleep(100);
                                    continue;
                                }
                                break;
                            }
                        } else {
                            entry = (Entry) eventObj;
                        }
                        if (entry != null) {
                            String currentBinlogFileName = entry.getHeader().getLogfileName();
                            updateDbAddress(lastBinlogFileName, currentBinlogFileName);
                            lastBinlogFileName = currentBinlogFileName;
                            LogPosition parsePosition = dbSyncReadJob.buildLastPosition(entry, curDbAddress);
                            if ((entry.getEntryType() != EntryType.TRANSACTIONBEGIN)
                                    && (entry.getEntryType() != EntryType.TRANSACTIONEND)) {
                                if (firstEventPos == null) {
                                    firstEventPos = dbSyncReadJob.buildLastPosition(entry, curDbAddress);
                                }
                                /*
                                 * 过滤模式，不做消费处理，拉取偏移量往前走
                                 */
                                if (DBSyncJobConf.RUNNING_MODEL_FILTER == dbSyncReadJob.getJobRunningModel()) {
                                    updateCachePosition(parsePosition);
                                } else {
                                    sendData(entry, parseMsgId, parsePosition);
                                }
                            } else {
                                /*
                                 * 走一遍位点生产、消费逻辑更新最新位点
                                 */
                                ;
                                updateCachePosition(parsePosition);
                            }
                        }
                        parseMsgId++;
                    } catch (Throwable e) {
                        if (bParseRunning || LOGGER.isDebugEnabled()) {
                            LOGGER.error("{}, {} async parse binlog Error", jobName, parserJobName, e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("{} {} get binlog error", jobName, parserJobName, e);
            } catch (Throwable e) {
                LOGGER.error("{}, {} get binlog error", jobName, parserJobName, e);
            }
        }
        LOGGER.info("stop parse");
        parseStatus = JobStat.State.STOP;
    }

    private void sendData(Entry entry, long parseMsgId, LogPosition parsePosition) {

        String dbName = entry.getHeader().getSchemaName();
        String tbName = entry.getHeader().getTableName();

        List<MysqlTableConf> myConfList;
        if (dbSyncReadJob.dbSyncJobConf.bInNeedTable(dbName, tbName)
                && (myConfList = dbSyncReadJob.dbSyncJobConf.getMysqlTableConfList(dbName, tbName)) != null) {
            AtomicInteger sendIndex = new AtomicInteger(0);
            for (MysqlTableConf myConf : myConfList) {
                // multi business report
                RowChange rowChange;
                try {
                    rowChange = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry, e);
                }
                EventType eventType = rowChange.getEventType();
                if (eventType == EventType.QUERY) {
                    continue;
                } else if (rowChange.getIsDdl()) {
                    String sql = rowChange.getSql();
                    if (StringUtils.isNotEmpty(sql) && sql.toLowerCase().contains("add")
                            && sql.toLowerCase().contains("alter")) {
                        DbColumnChangeManager.getInstance().addAlterFieldString(myConf.getGroupId(), myConf.getTaskId(),
                                sql);
                    }
                    continue;
                } else {
                    sendDataByPbProtoc(rowChange, entry, dbName, tbName, myConf,
                            parsePosition, parseMsgId, dbSyncReadJob.dbSyncJobConf.getDbJobId(), readJobPositionManager,
                            sendIndex);
                }
            }
        }
    }

    private boolean sendDataByPbProtoc(RowChange rowChange, Entry entry, String dbName, String tbName,
            MysqlTableConf myConf, LogPosition logPosition, long parseMsgId,
            String dbJobId, ReadJobPositionManager readJobPositionManager,
            AtomicInteger sendIndex) {
        if (rowChange == null) {
            return false;
        }
        EventType eventType = rowChange.getEventType();

        if (eventType == EventType.QUERY || rowChange.getIsDdl()) {
            return false;
        }

        // skip delete rowdata
        boolean isSkipData = AgentConfiguration.getAgentConf()
                .getBoolean(DBSYNC_NEED_SKIP_DELETE_DATA, DEFAULT_DBSYNC_NEED_SKIP_DELETE_DATA);
        if (eventType == EventType.DELETE && (isSkipData || myConf.isSkipDelete())) {
            return false;
        }

        int needSize = rowChange.getRowDatasList().size();
        if (needSize > 0) {
            long timeStample = 0;
            /*
             * 为了对齐审计数据，1 分钟一个纬度做数据聚合，可能会导致pulsar 数据量增加
             */
            long execTime = entry.getHeader().getExecuteTime();
            if (execTime > 0) {
                timeStample = execTime - (execTime % (1000 * 60 * 1));
            } else {
                execTime = Instant.now().toEpochMilli();
                timeStample = execTime - (execTime % (1000 * 60 * 1));
            }
            HashMap<String, String> headerMap = new HashMap<>();
            headerMap.put(CommonConstants.PROXY_KEY_DATA, String.valueOf(timeStample));
            headerMap.put(CommonConstants.PROXY_KEY_DATE, String.valueOf(timeStample));
            headerMap.put(CommonConstants.PROXY_KEY_GROUP_ID, myConf.getGroupId());
            headerMap.put(CommonConstants.PROXY_KEY_STREAM_ID, myConf.getStreamId());
            String pbInstName = getPbInstName(logPosition);
            String jobName = dbSyncReadJob.getDbSyncJobConf().getDbJobId();
            for (RowData rowData : rowChange.getRowDatasList()) {
                LogPosition sendPosition = new LogPosition(logPosition);
                sendPosition.setSendIndex(sendIndex.getAndIncrement());
                sendPosition.setPkgIndex(parseMsgId);
                sendPosition.setParseThreadName(this.parserJobName);
                byte[] body = parsePbData(rowData, dbName, tbName, entry.getHeader().getExecuteTime(),
                        eventType, parseMsgId, dbJobId, pbInstName);
                DBSyncMessage message = new DBSyncMessage(body, headerMap, jobName, timeStample);
                message.setLogPosition(sendPosition);
                message.setMsgId(parseMsgId);
                dbSyncReadJob.addMessage(myConf.getTaskId(), message);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] send message task {} position = {}", jobName, myConf.getTaskId(),
                            sendPosition.toMinString());
                }
                readJobPositionManager.addLogPositionToSendCache(sendPosition);
            }
            return true;
        }
        return false;
    }

    private String getPbInstName(LogPosition logPosition) {
        String pbInstName = "";
        if (logPosition != null && logPosition.getIdentity() != null) {
            InetSocketAddress address = logPosition.getIdentity().getSourceAddress();
            if (address != null) {
                pbInstName = address.toString();
            }
        }
        if (StringUtils.isEmpty(pbInstName)) {
            pbInstName = dbSyncReadJob.getCurrentDbIpAddress();
        }
        return pbInstName;
    }
    private byte[] parsePbData(RowData rowData, String dbName, String tbName, long execTime,
            EventType eventType, long msgIdIndex, String dbJobId, String pbInstName) {
        RowData.Builder rowDataOrBuilder = rowData.toBuilder();
        String schemaName = Joiner.on(ipSep)
                .join(dbName, AgentUtils.getLocalIp(),
                        snowFlakeManager.generateSnowId(DBSyncUtils.serverId2Int(dbJobId)));
        rowDataOrBuilder.setInstanceName(pbInstName);
        rowDataOrBuilder.setSchemaName(schemaName);
        rowDataOrBuilder.setTableName(tbName);
        rowDataOrBuilder.setExecuteTime(execTime);
        rowDataOrBuilder.setExecuteOrder(msgIdIndex);
        rowDataOrBuilder.setEventType(eventType);
        rowDataOrBuilder.setTransferIp(AgentUtils.getLocalIp());
        return rowDataOrBuilder.build().toByteArray();
    }

    private void updateDbAddress(String lastBinlogFileName, String curBinlogFileName) {
        if (newDbAddress != null && StringUtils.isNotEmpty(curBinlogFileName)
                && !Objects.equals(lastBinlogFileName, curBinlogFileName)) {
            curDbAddress = newDbAddress;
            newDbAddress = null;
        }
    }

    private void handleHeartbeatEvent(LogEvent event, long pkgEventTimeStamp, long parseMsgId) {
        if (event.getHeader().getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
            if (newDbAddress != null) {
                curDbAddress = newDbAddress;
                newDbAddress = null;
            }
            String binlogFilename = ((HeartbeatLogEvent) event).getLogIdent();
            long timeStamp = event.getWhen() * 1000;
            if (timeStamp == 0) {
                timeStamp = pkgEventTimeStamp * 1000;
            }
            LogPosition heartBeatLogPosition = new LogPosition();
            heartBeatLogPosition.setPosition(new EntryPosition(binlogFilename,
                    event.getLogPos(), timeStamp,
                    event.getServerId()));
            LogIdentity identity = new LogIdentity(curDbAddress, -1L);
            heartBeatLogPosition.setIdentity(identity);

            heartBeatLogPosition.setSendIndex(0);
            heartBeatLogPosition.setPkgIndex(parseMsgId);
            heartBeatLogPosition.setParseThreadName(parserJobName);

            /*
             * 心跳数据走一下发送，确认流程，用于处理当前的解析位置
             */
            updateCachePosition(heartBeatLogPosition);
        }
    }

    public synchronized void updateCurrentDbAddress(InetSocketAddress newDbAddress) {
        if (newDbAddress != null) {
            this.newDbAddress = newDbAddress;
        }
    }

    /*
     * 走一遍位点生产、消费逻辑更新最新位点
     */
    private void updateCachePosition(LogPosition position) {
        readJobPositionManager.addLogPositionToSendCache(position);
        readJobPositionManager.ackSendPosition(position);
    }

    public void putEvents(PkgEvent events) {
        queue.offer(events);
    }

    public boolean bParseQueueIsFull() {
        return queue.size() > 100;
    }

    public void stopParse() {
        LOGGER.info("stop parse");
        bParseRunning = false;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public JobStat.State getParserStatus() {
        return parseStatus;
    }

    public void updateCharSet(Charset charSet) {
        ((LogEventConvert) dbSyncReadJob.getBinlogParser()).setCharset(charSet);
    }

    @Override
    public String report() {
        return null;
    }
}
