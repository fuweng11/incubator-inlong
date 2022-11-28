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

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.protocol.CanalEntry;
import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.common.protocol.CanalEntry.EntryType;
import org.apache.inlong.agent.common.protocol.CanalEntry.RowChange;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.EventType;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.RowData;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.core.FieldManager;
import org.apache.inlong.agent.message.DBSyncMessage;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.HeartbeatLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.LogEventConvert;
import org.apache.inlong.agent.mysql.connector.dbsync.TableMetaCache;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_NEED_SKIP_DELETE_DATA;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_DBSYNC_NEED_SKIP_DELETE_DATA;

public class ParseThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParseThread.class);
    private final String parserJobName;
    private final LinkedBlockingQueue<PkgEvent> queue;
    private final SnowFlakeManager snowFlakeManager = new SnowFlakeManager();
    private final PositionControl positionControl;
    private LogPosition lastParsePosition;
    private LogPosition lastProcessedPosition;
    @SuppressWarnings("rawtypes")
    private MysqlConnection parseMetaConnection;
    private volatile boolean bParseRunning = false;
    private JobStat.State parseStatus;
    private InetSocketAddress dbAddress;
    private final DBSyncReadOperator dbSyncReadOperator;
    private String jobName;
    private char ipSep = '@';
    private char sep = ',';

    public ParseThread(String parserName, MysqlConnection parseMetaConnection, DBSyncReadOperator dbSyncReadOperator) {
        this.parseMetaConnection = parseMetaConnection;
        this.parserJobName = parserName;
        this.dbAddress = parseMetaConnection.getConnector().getAddress();
        this.dbSyncReadOperator = dbSyncReadOperator;
        this.jobName = dbSyncReadOperator.getJobName();
        TableMetaCache tableMetaCache = new TableMetaCache(this.parseMetaConnection);
        ((LogEventConvert) dbSyncReadOperator.getBinlogParser()).setTableMetaCache(tableMetaCache);
        queue = new LinkedBlockingQueue<>();
        super.setUncaughtExceptionHandler((t, e) -> {
            parseStatus = JobStat.State.STOP;
            LOGGER.error("{} Parse Thread has an uncaught error {}, \n {}",
                    jobName, parserJobName, DBSyncUtils.getExceptionStack(e));
        });
        parseStatus = JobStat.State.INIT;
        positionControl = dbSyncReadOperator.getPositionControl();
    }

    public void start() {
        super.setName(parserJobName);
        bParseRunning = true;
        super.start();
    }

    @SuppressWarnings("unchecked")
    public void run() {
        PkgEvent pkgEvent = null;
        ArrayList<LogEvent> eventList = null;
        parseStatus = JobStat.State.RUN;
        long parseMsgId = 0;
        boolean bInTransEnd = true;
        LOGGER.info("{}, {} begin running!", jobName, this.parserJobName);
        while (bParseRunning || !bInTransEnd || !queue.isEmpty()) {
            try {
                pkgEvent = queue.poll(1, TimeUnit.SECONDS);

                if (pkgEvent == null) {
                    if (!bParseRunning && dbSyncReadOperator.getParseDisptcherStatus() == JobStat.State.STOP
                            && queue.isEmpty() && !bInTransEnd) {
                        bInTransEnd = true;
                        LOGGER.error("{}, {} stopped occure trans end event missing!!!",
                                jobName, parserJobName);
                    }
                    continue;
                }
                eventList = pkgEvent.getEventLists();
                parseMsgId = pkgEvent.getIndex();
                bInTransEnd = pkgEvent.isbHasTransEnd();
                boolean bNeedFlush = false;

                ArrayList<Entry> entryList = new ArrayList<>();

                LogPosition firstEventPos = null;
                for (LogEvent event : eventList) {
                    dbSyncReadOperator.setHeartBeat(System.currentTimeMillis());
                    Entry entry = null;
                    // Query -> Rows_query -> Table_map -> Update_rows -> Xid
                    if (!bParseRunning) {
                        break;
                    }
                    try {
                        int retryCnt = 0;
                        while (true) {
                            try {
                                if (retryCnt > 10 || !bParseRunning) {
                                    LOGGER.error("{}, {} retry 10, now skip an event, pos {} !",
                                            jobName, parserJobName, event.getLogPos());
                                    MonitorLogUtils.printEventDiscard(dbSyncReadOperator.getCurrentDbInfo(),
                                            MonitorLogUtils.EVENT_DISCARD_EXCEED_RETRY_CNT, "");
                                    break;
                                }
                                entry = dbSyncReadOperator.getBinlogParser().parse(event, dbSyncReadOperator.jobconf);

                            } catch (TableIdNotFoundException | TableMapEventMissException tie) {
                                // need redump, refind the beginning of transaction and parse
                                LOGGER.error("async parse binlog error: ", tie);
                                MonitorLogUtils.printEventDiscard(dbSyncReadOperator.getCurrentDbInfo(),
                                        MonitorLogUtils.EVENT_DISCARD_TABLE_ID_NOT_FOUND, "");
                                throw tie;
                            } catch (CanalParseException te) {
                                LOGGER.error("async parse binlog error: ", te);
                                try {
                                    synchronized (parseMetaConnection) {
                                        parseMetaConnection.reconnect();
                                    }
                                } catch (IOException ioe) {
                                    LOGGER.error("{}, {} reconnect meta connection error", jobName, parserJobName, ioe);
                                }
                                LOGGER.error("{}, {} parse binlog event error ", jobName, parserJobName, te);
                                retryCnt++;
                                DBSyncUtils.sleep(100);
                                continue;
                            }
                            break;
                        }
                        if (entry != null) {
                            this.lastParsePosition = dbSyncReadOperator.buildLastPosition(entry, dbAddress);
                            positionControl.addEventLogPosition(lastParsePosition);
                            if ((entry.getEntryType() != EntryType.TRANSACTIONBEGIN)
                                    && (entry.getEntryType() != EntryType.TRANSACTIONEND)) {
                                bNeedFlush = true;
                                entryList.add(entry);
                                if (firstEventPos == null) {
                                    firstEventPos = lastParsePosition;
                                }
                                constructMessage(entry, parseMsgId);
                            }
                            lastProcessedPosition = lastParsePosition;
                            positionControl.removeEventLogPosition(lastParsePosition);
                        } else if (event.getHeader().getType() == LogEvent.HEARTBEAT_LOG_EVENT) {
                            String binlogFilename = ((HeartbeatLogEvent) event).getLogIdent();
                            long timeStamp = event.getWhen() * 1000;
                            if (timeStamp == 0) {
                                timeStamp = Instant.now().toEpochMilli();
                            }
                            LogPosition heartBeatLogPosition = new LogPosition();

                            heartBeatLogPosition.setPosition(new EntryPosition(binlogFilename,
                                    event.getLogPos(), timeStamp,
                                    event.getServerId()));
                            LogIdentity identity = new LogIdentity(dbAddress, -1L);
                            heartBeatLogPosition.setIdentity(identity);

                            heartBeatLogPosition.setSendIndex(0);
                            heartBeatLogPosition.setPkgIndex(parseMsgId);
                            heartBeatLogPosition.setParseThreadName(parserJobName);

                            lastProcessedPosition = heartBeatLogPosition;
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("parserJobName = {} lastProcessedPosition = {}",
                                        parserJobName, lastProcessedPosition);
                            }
                        }
                        parseMsgId++;
                    } catch (Throwable e) {
                        bNeedFlush = false;
                        if (bParseRunning || LOGGER.isDebugEnabled()) {
                            LOGGER.error("{}, {} async parse binlog Error", jobName, parserJobName, e);
                        }
                    }
                }
                // record the last dump transaction event
                if (bNeedFlush) {
                    flushLastPkgEvent(pkgEvent, entryList, firstEventPos);
                    firstEventPos = null;
                }

            } catch (InterruptedException e) {
                LOGGER.error("{} {} get binlog error", jobName, parserJobName, e);
            } catch (Throwable e) {
                LOGGER.error("{}, {} get binlog error", jobName, parserJobName, e);
            }
        }
        LOGGER.info("stop parse");

        parseStatus = JobStat.State.STOP;

        if (lastParsePosition != null) {
            LOGGER.warn("{}, {} stop position : {}", jobName, parserJobName, lastParsePosition.getJsonObj().toString());
        }
    }

    private synchronized void flushLastPkgEvent(PkgEvent pkgEvent, ArrayList<CanalEntry.Entry> entryList,
            LogPosition parsePos) {
        if (dbSyncReadOperator.lastPkgEvent == null
                || dbSyncReadOperator.lastPkgEvent.getIndex() < pkgEvent.getIndex()) {
            dbSyncReadOperator.lastPkgEvent = pkgEvent;
            dbSyncReadOperator.lastEntryList = entryList;
            dbSyncReadOperator.lastParsePos = new LogPosition(parsePos);
        }
    }

    private void constructMessage(Entry entry, long parseMsgId) {

        String dbName = entry.getHeader().getSchemaName();
        String tbName = entry.getHeader().getTableName();

        List<MysqlTableConf> myConfList = null;
        if (dbSyncReadOperator.jobconf.bInNeedTable(dbName, tbName)
                && (myConfList = dbSyncReadOperator.jobconf.getMysqlTableConfList(dbName, tbName)) != null) {
            int sendIndex = 0;
            for (MysqlTableConf myConf : myConfList) {
                // multi business report
                LogPosition sendPosition = new LogPosition(lastParsePosition);
                sendPosition.setSendIndex(sendIndex);
                sendPosition.setPkgIndex(parseMsgId);
                sendPosition.setParseThreadName(this.parserJobName);
                RowChange rowChange = null;
                try {
                    rowChange = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                EventType eventType = rowChange.getEventType();
                if (eventType == EventType.QUERY) {
                    continue;
                } else if (rowChange.getIsDdl()) {
                    String sql = rowChange.getSql();
                    if (StringUtils.isNotEmpty(sql) && sql.toLowerCase().contains("add")) {
                        FieldManager.getInstance().addAlterFieldString(myConf.getGroupId(), myConf.getTaskId(), sql);
                    }
                    continue;
                } else {
                    positionControl.addLogPositionToSendCache(sendPosition);

                    boolean putResult = genSendDataByPbProtoc(rowChange, entry, dbName, tbName, myConf,
                            sendPosition, parseMsgId, dbSyncReadOperator.jobconf.getServerId());
                    // if (putResult) {
                    // //TODO:puting data first is ok?
                    // dbSyncReadOperator.addLogPositionToCache(sendPosition);
                    // }
                }
                sendIndex++;
            }
        }
    }

    private boolean genSendDataByPbProtoc(RowChange rowChange, Entry entry, String dbName, String tbName,
            MysqlTableConf myConf, LogPosition logPosition, long parseMsgId, String serverId) {
        if (rowChange == null) {
            return false;
        }
        EventType eventType = rowChange.getEventType();
        String iname = myConf.getStreamId();

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
            for (RowData rowData : rowChange.getRowDatasList()) {
                parsePbData(rowData, dbName, tbName, entry.getHeader().getExecuteTime(),
                        eventType, parseMsgId, serverId, logPosition, myConf.getTaskId());
            }
            // TODO: is need?
            // if (isDebug) {
            // String address = logPosition.getIdentity().getSourceAddress().toString().replaceAll("/", "");
            // long timeStample = entry.getHeader().getExecuteTime() - entry.getHeader().getExecuteTime() % 600000;
            // String key = "job_debug_info#" + myConf.getBusinessId() + "#" + iname + "#" + address
            // + "#" + DBSyncUtils.convertFrom(timeStample).format(fmt);
            // sendMsgCnt.add(key, logData.getDataList().size());
            // }
            return true;

        }

        return false;
    }

    private void parsePbData(RowData rowData, String dbName, String tbName, long execTime,
            EventType eventType, long msgIdIndex, String serverId, LogPosition logPosition, Integer taskId) {
        RowData.Builder rowDataOrBuilder = rowData.toBuilder();

        String jobName = dbSyncReadOperator.getJobconf().getJobName();
        String pbInstName = jobName.substring(0, jobName.lastIndexOf(":"));
        String schemaName = Joiner.on(ipSep)
                .join(dbName, AgentUtils.getLocalIp(),
                        snowFlakeManager.generateSnowId(DBSyncUtils.serverId2Int(serverId)));
        rowDataOrBuilder.setInstanceName(pbInstName);
        rowDataOrBuilder.setSchemaName(schemaName);
        rowDataOrBuilder.setTableName(tbName);
        rowDataOrBuilder.setExecuteTime(execTime);
        rowDataOrBuilder.setExecuteOrder(msgIdIndex);
        rowDataOrBuilder.setEventType(eventType);
        rowDataOrBuilder.setTransferIp(AgentUtils.getLocalIp());
        String key = dbName + sep + tbName;

        // logData.addData(rowDataOrBuilder.build().toByteArray(), key, execTime);
        DBSyncMessage message = new DBSyncMessage(rowDataOrBuilder.build().toByteArray()); // add groupId-streamId
        message.setInstName(pbInstName);
        message.setLogPosition(logPosition);
        message.setMsgId(msgIdIndex);
        dbSyncReadOperator.addMessage(taskId, message);
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

    public LogPosition getLastProcessedPosition() {
        return this.lastProcessedPosition;
    }

    public LogPosition getLastParsPosition() {
        return this.lastParsePosition;
    }

    public void updateCharSet(Charset charSet) {
        ((LogEventConvert) dbSyncReadOperator.getBinlogParser()).setCharset(charSet);
    }
}
