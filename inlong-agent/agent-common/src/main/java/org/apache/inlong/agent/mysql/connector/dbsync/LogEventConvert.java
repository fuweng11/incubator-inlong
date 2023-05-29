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

package org.apache.inlong.agent.mysql.connector.dbsync;

import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.common.protocol.CanalEntry.EntryType;
import org.apache.inlong.agent.common.protocol.CanalEntry.Header;
import org.apache.inlong.agent.common.protocol.CanalEntry.RowChange;
import org.apache.inlong.agent.common.protocol.CanalEntry.TransactionBegin;
import org.apache.inlong.agent.common.protocol.CanalEntry.TransactionEnd;
import org.apache.inlong.agent.common.protocol.CanalEntry.Type;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.Column;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.EventType;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.Pair;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.RowData;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.DeleteRowsLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.IntvarLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.LogHeader;
import org.apache.inlong.agent.mysql.connector.binlog.event.QueryLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.RandLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.RotateLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.RowsLogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.event.RowsLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.RowsQueryLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.TableMapLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.TableMapLogEvent.ColumnInfo;
import org.apache.inlong.agent.mysql.connector.binlog.event.UnknownLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.UpdateRowsLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.UserVarLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.WriteRowsLogEvent;
import org.apache.inlong.agent.mysql.connector.binlog.event.XidLogEvent;
import org.apache.inlong.agent.mysql.connector.dbsync.SimpleDdlParser.DdlResult;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.connector.exception.TableIdNotFoundException;
import org.apache.inlong.agent.mysql.connector.exception.TableMapEventMissException;
import org.apache.inlong.agent.mysql.filter.CanalEventFilter;
import org.apache.inlong.agent.mysql.parse.BinlogParser;
import org.apache.inlong.agent.mysql.parse.TableMeta;
import org.apache.inlong.agent.mysql.parse.TableMeta.FieldMeta;
import org.apache.inlong.agent.mysql.utils.AbstractCanalLifeCycle;
import org.apache.inlong.agent.utils.JsonParser;
import org.apache.inlong.agent.utils.JsonParser.JsonValue;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.List;

import static org.apache.inlong.agent.constant.AgentConstants.DBSYNC_MAX_COLUMN_VALUE_SIZE;
import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_MAX_COLUMN_VALUE_SIZE;

public class LogEventConvert extends AbstractCanalLifeCycle implements BinlogParser<LogEvent> {

    public static final String ISO_8859_1 = "ISO-8859-1";
    public static final String UTF_8 = "UTF-8";
    public static final int TINYINT_MAX_VALUE = 256;
    public static final int SMALLINT_MAX_VALUE = 65536;
    public static final int MEDIUMINT_MAX_VALUE = 16777216;
    public static final long INTEGER_MAX_VALUE = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");
    public static final int VERSION = 1;
    public static final String BEGIN = "BEGIN";
    public static final String COMMIT = "COMMIT";
    public static final Logger LOGGER = LogManager.getLogger(LogEventConvert.class);
    public static final int MILLISEC_IN_ONE_SEC = 1000;
    private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHH");
    public int maxColumnValueLen = 1024 * 1024;
    private volatile CanalEventFilter<String> nameFilter;
    private TableMetaCache tableMetaCache;
    private String binlogFileName = "mysql-bin.000001";
    private Charset charset = Charset.defaultCharset();
    private boolean bRecvTabMapEvent = false;
    private String clearedDate;

    public Entry parse(LogEvent logEvent, DBSyncJobConf jobconf) throws CanalParseException {
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                String tmpClearDate = LocalDateTime.now().format(fmt);
                if (clearedDate == null) {
                    clearedDate = tmpClearDate;
                } else {
                    String clearDay = clearedDate.substring(0, 8);
                    String tmpClearDay = tmpClearDate.substring(0, 8);
                    if (clearDay.compareTo(tmpClearDay) < 0 && tmpClearDate.substring(8).compareTo("03") >= 0) {
                        if (tableMetaCache != null) {
                            tableMetaCache.clearTableMeta();
                            LOGGER.info("{} clear the tableMetaCache in {} !",
                                    jobconf.getJobName(), tmpClearDate);
                        }
                        clearedDate = tmpClearDate;
                    }
                }
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                return parseQueryEvent((QueryLogEvent) logEvent);
            case LogEvent.ROWS_QUERY_LOG_EVENT:
                return parseRowsQueryEvent((RowsQueryLogEvent) logEvent);
            case LogEvent.XID_EVENT:
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                bRecvTabMapEvent = true;
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                return parseRowsEvent((WriteRowsLogEvent) logEvent, jobconf);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
                return parseRowsEvent((UpdateRowsLogEvent) logEvent, jobconf);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                return parseRowsEvent((DeleteRowsLogEvent) logEvent, jobconf);
            case LogEvent.USER_VAR_EVENT:
                return parseUserVarLogEvent((UserVarLogEvent) logEvent);
            case LogEvent.INTVAR_EVENT:
                return parseIntrvarLogEvent((IntvarLogEvent) logEvent);
            case LogEvent.RAND_EVENT:
                return parseRandLogEvent((RandLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    public void reset(boolean bCleanTableMap) {
        // do nothing
        binlogFileName = "mysql-bin.000001";
        if (tableMetaCache != null && bCleanTableMap) {
            tableMetaCache.clearTableMeta();
            bRecvTabMapEvent = false;
        }
        // bRecvTabMapEvent = false;
    }

    private Entry parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            TransactionBegin transactionBegin = createTransactionBegin(event.getExecTime());
            Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            TransactionEnd transactionEnd = createTransactionEnd(0L, event.getWhen());
            Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
        } else {
            // DDL
            DdlResult result = SimpleDdlParser.parse(queryString, event.getDbName());

            String schemaName = event.getDbName();
            if (StringUtils.isEmpty(schemaName) && StringUtils.isNotEmpty(result.getSchemaName())) {
                schemaName = result.getSchemaName();
            }

            String tableName = result.getTableName();
            if (tableMetaCache != null && (result.getType() == EventType.ALTER
                    || result.getType() == EventType.ERASE)) {
                if (StringUtils.isNotEmpty(tableName)) {
                    // if get right table info, use fullName to clear
                    tableMetaCache.clearTableMetaWithFullName(schemaName + "." + tableName);
                } else {
                    // if can't get right table info, use schema to clear
                    tableMetaCache.clearTableMetaWithSchemaName(schemaName);
                }
            }

            final Header header = createHeader(binlogFileName, event.getHeader(), schemaName, tableName,
                    result.getType());
            RowChange.Builder rowChangeBuider = RowChange.newBuilder();
            if (result.getType() != EventType.QUERY) {
                rowChangeBuider.setIsDdl(true);
            }
            if (EventType.ALTER == result.getType()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("EventType = {} queryString = {}", EventType.ALTER, queryString);
                }

            }
            rowChangeBuider.setSql(queryString);
            rowChangeBuider.setEventType(result.getType());
            return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        }
    }

    private Entry parseRowsQueryEvent(RowsQueryLogEvent event) {
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(StandardCharsets.ISO_8859_1),
                    charset.name());
            return buildQueryEntry(queryString, event.getHeader());
        } catch (UnsupportedEncodingException e) {
            throw new CanalParseException(e);
        }
    }

    private Entry parseUserVarLogEvent(UserVarLogEvent event) {
        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseIntrvarLogEvent(IntvarLogEvent event) {
        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseRandLogEvent(RandLogEvent event) {
        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseXidEvent(XidLogEvent event) {
        TransactionEnd transactionEnd = createTransactionEnd(event.getXid(), event.getWhen());
        Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
    }

    private Entry parseRowsEvent(RowsLogEvent event, DBSyncJobConf jobconf) {
        maxColumnValueLen = AgentConfiguration.getAgentConf()
                .getInt(DBSYNC_MAX_COLUMN_VALUE_SIZE, DEFAULT_MAX_COLUMN_VALUE_SIZE);
        try {
            TableMapLogEvent table = event.getTable();
            if (table == null) {
                if (bRecvTabMapEvent) {
                    // record of tableId doesn't exist
                    throw new TableIdNotFoundException("not found tableId:" + event.getTableId());
                } else {
                    throw new TableMapEventMissException("Miss a TABLE_MAP_EVENT, so not found tableId "
                            + event.getTableId());
                }
            }

            String fullname = getSchemaNameAndTableName(table);

            if (nameFilter != null && !nameFilter.filter(fullname)) { // check name filter
                return null;
            }
            EventType eventType = null;
            int type = event.getHeader().getType();
            if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                eventType = EventType.INSERT;
            } else if (LogEvent.UPDATE_ROWS_EVENT_V1 == type || LogEvent.UPDATE_ROWS_EVENT == type) {
                eventType = EventType.UPDATE;
            } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                eventType = EventType.DELETE;
            } else {
                throw new CanalParseException("unsupport event type :" + event.getHeader().getType());
            }

            String dbName = table.getDbName();
            String tbName = table.getTableName();

            RowChange.Builder rowChangeBuider = RowChange.newBuilder();
            rowChangeBuider.setTableId(event.getTableId());
            rowChangeBuider.setIsDdl(false);

            rowChangeBuider.setEventType(eventType);
            Charset taskCharset = getCharset(jobconf, dbName, tbName);
            RowsLogBuffer buffer = event.getRowsBuf(taskCharset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            boolean tableError = false;
            TableMeta tableMeta = null;
            if (tableMetaCache != null) {
                tableMeta = tableMetaCache.getTableMeta(fullname);
                if (tableMeta == null) {
                    throw new CanalParseException("not found [" + fullname + "] in db , pls check!");
                }
            }

            while (buffer.nextOneRow(columns)) {
                // handle row record
                RowData.Builder rowDataBuilder = RowData.newBuilder();
                // RowData rowDataBuilder = new RowData();
                if (EventType.INSERT == eventType) {
                    // put insert in before value
                    parseOneRow(rowDataBuilder, event, buffer, columns, true, tableMeta, jobconf, dbName, tbName);
                } else if (EventType.DELETE == eventType) {
                    // put delete in before
                    parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta, jobconf, dbName, tbName);
                } else {
                    // put update in before and after
                    parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta, jobconf, dbName, tbName);
                    if (!buffer.nextOneRow(changeColumns)) {
                        rowChangeBuider.addRowDatas(rowDataBuilder);
                        break;
                    }

                    parseOneRow(rowDataBuilder, event, buffer, changeColumns, true, tableMeta, jobconf, dbName, tbName);
                }

                rowChangeBuider.addRowDatas(rowDataBuilder);
            }
            Header header = createHeader(binlogFileName, event.getHeader(), table.getDbName(),
                    table.getTableName(), eventType);
            return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        } catch (TableIdNotFoundException | TableMapEventMissException tie) {
            throw tie;
        } catch (Exception e) {
            LOGGER.error("Parse row data failed!", e);
            throw new CanalParseException("parse row data failed.", e);
        }
    }

    private void parseOneRow(RowData.Builder rowDataBuilder, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols,
            boolean isAfter, TableMeta tableMeta, DBSyncJobConf jobconf, String dbName, String tbName) {
        final int columnCnt = event.getTable().getColumnCnt();
        final ColumnInfo[] columnInfo = event.getTable().getColumnInfo();

        // check table fileds count, only supporting add fields
        if (tableMeta != null && columnInfo.length > tableMeta.getFileds().size()) {
            String fullname = getSchemaNameAndTableName(event.getTable());
            tableMeta = tableMetaCache.getTableMeta(fullname, false);
            if (tableMeta == null) {
                throw new CanalParseException("not found [" + fullname + "] in db , pls check!");
            }

            if (columnInfo.length > tableMeta.getFileds().size()) {

                LOGGER.error("skip a event parse : {}, {}, because fields not match {} : {}",
                        dbName, tbName, columnInfo.length, tableMeta.getFileds().size());
            }
        }

        boolean isOverMax = false;
        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = columnInfo[i];
            if (!cols.get(i)) {
                continue;
            }
            buffer.nextValue(info.type, info.meta);

            FieldMeta fieldMeta = null;
            boolean isOverride = false;
            Column.Builder columnBuilder = Column.newBuilder();
            // Column columnBuilder = new Column();
            columnBuilder.setIndex(i);
            columnBuilder.setIsNull(false);
            if (tableMeta != null) {
                // handle file meta
                if (i < tableMeta.getFileds().size()) {
                    fieldMeta = tableMeta.getFileds().get(i);
                    columnBuilder.setName(fieldMeta.getColumnName());
                    columnBuilder.setIsKey(fieldMeta.isKey());
                } else {
                    isOverride = true;
                    columnBuilder.setName("TDBANK_UNKNOWN_FIELDS_" + i);
                    columnBuilder.setIsKey(false);
                }
            }
            int javaType = isOverride ? Types.VARCHAR : buffer.getJavaType();
            if (buffer.isNull()) {
                columnBuilder.setIsNull(true);
                columnBuilder.setLength(0);
            } else {
                final Serializable value = buffer.getValue();
                columnBuilder.setLength(buffer.getLength());
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        // unsigned type
                        Number number = (Number) value;
                        if (fieldMeta != null && fieldMeta.isUnsigned() && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1: /* MYSQL_TYPE_TINY */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.SMALLINT;
                                    break;

                                case 2: /* MYSQL_TYPE_SHORT */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER;
                                    break;

                                case 3: /* MYSQL_TYPE_INT24 */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER;
                                    break;

                                case 4: /* MYSQL_TYPE_LONG */
                                    columnBuilder.setValue(String.valueOf(Long.valueOf(INTEGER_MAX_VALUE
                                            + number.longValue())));
                                    javaType = Types.BIGINT;
                                    break;

                                case 8: /* MYSQL_TYPE_LONGLONG */
                                    columnBuilder.setValue(
                                            BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue())).toString());
                                    javaType = Types.DECIMAL;
                                    break;
                            }
                        } else {
                            columnBuilder.setValue(String.valueOf(value));
                        }
                        break;
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                    case Types.BIT:// bit
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIME:
                    case Types.DATE:
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        if (fieldMeta != null && isText(fieldMeta.getColumnType())) {
                            javaType = Types.CLOB;
                        } else {
                            javaType = Types.BLOB;
                        }
                        if (fieldMeta != null && isJSON(fieldMeta.getColumnType())) {
                            byte[] valueList = (byte[]) value;
                            LogBuffer buf = new LogBuffer(valueList, 0, valueList.length);
                            JsonValue jsonValue = JsonParser.parse_value(buf.getUint8(), buf, buffer.getLength() - 1);
                            StringBuilder builder = new StringBuilder();
                            jsonValue.toJsonString(builder);
                            columnBuilder.setValue(builder.toString());
                        } else {
                            byte[] valueList = (byte[]) value;
                            if (valueList.length > maxColumnValueLen) {
                                if (!isOverMax) {
                                    byte[] truncValue = new byte[maxColumnValueLen];
                                    System.arraycopy(valueList, 0, truncValue, 0, maxColumnValueLen);
                                    columnBuilder.setValue(new String(Base64.encodeBase64(truncValue, false)));
                                    isOverMax = true;
                                    LOGGER.warn("column {} length over max {}, cut it.",
                                            columnBuilder.getName(), maxColumnValueLen);
                                } else {
                                    columnBuilder.setValue(new String(Base64.encodeBase64(" ".getBytes(), false)));
                                    LOGGER.warn("column {} length over max {}, set to empty.",
                                            columnBuilder.getName(), maxColumnValueLen);
                                }
                                MysqlTableConf mysqlTableConf = jobconf.getMysqlTableConf(dbName, tbName);
                                // TODO:add send metrics
                                // DBSyncUtils.sendBigFieldMetrics(event.getWhen() * MILLISEC_IN_ONE_SEC,
                                // mysqlTableConf,
                                // fieldMeta, valueList.length);
                            } else {
                                columnBuilder.setValue(new String(Base64.encodeBase64(valueList, false)));
                            }
                        }

                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        columnBuilder.setValue(value.toString().replaceAll("\n", " ").replaceAll("\r", " ")
                                .replace((char) 0x01, (char) 0x00));
                        break;
                    default:
                        columnBuilder.setValue(value.toString().replaceAll("\n", " ").replaceAll("\r", " ")
                                .replace((char) 0x01, (char) 0x00));
                }

            }

            columnBuilder.setSqlType(javaType);
            // update flag
            columnBuilder.setUpdated(isAfter && isUpdate(rowDataBuilder.getBeforeColumnsList(),
                    columnBuilder.getIsNull() ? null : columnBuilder.getValue(), i));
            if (isAfter) {
                rowDataBuilder.addAfterColumns(columnBuilder);
            } else {
                rowDataBuilder.addBeforeColumns(columnBuilder);
            }
        }

    }

    private Entry buildQueryEntry(String queryString, LogHeader logHeader) {
        Header header = createHeader(binlogFileName, logHeader, "", "", EventType.QUERY);
        RowChange.Builder rowChangeBuider = RowChange.newBuilder();
        rowChangeBuider.setSql(queryString);
        rowChangeBuider.setEventType(EventType.QUERY);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
    }

    private String getSchemaNameAndTableName(TableMapLogEvent event) {
        return event.getDbName() + "." + event.getTableName();
    }

    private Header createHeader(String binlogFile, LogHeader logHeader, String schemaName,
            String tableName, EventType eventType) {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setVersion(VERSION);
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setServerenCode(UTF_8);
        headerBuilder.setExecuteTime(logHeader.getWhen() * 1000L);
        headerBuilder.setSourceType(Type.MYSQL);
        if (eventType != null) {
            headerBuilder.setEventType(eventType);
        }
        if (schemaName != null) {
            headerBuilder.setSchemaName(schemaName);
        }
        if (tableName != null) {
            headerBuilder.setTableName(tableName);
        }
        headerBuilder.setEventLength(logHeader.getEventLen());

        return headerBuilder.build();
    }

    private boolean isUpdate(List<Column> bfColumns, String newValue, int index) {
        if (bfColumns == null) {
            throw new CanalParseException("ERROR ## the bfColumns is null");
        }

        if (index < 0) {
            return false;
        }
        for (Column column : bfColumns) {
            // compare column inde of before/after
            if (column.getIndex() == index) {
                if (column.getIsNull() && newValue == null) {
                    // all null
                    return false;
                } else if (newValue != null && column.getValue().equals(newValue)) {
                    return false;
                }
            }
        }

        // nolob/minial, can't find before, mark it changed
        return true;
    }

    private boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
                || "TEXT".equalsIgnoreCase(columnType);
    }

    private boolean isJSON(String columnType) {
        return "JSON".equalsIgnoreCase(columnType);
    }

    public TransactionBegin createTransactionBegin(long executeTime) {
        TransactionBegin.Builder beginBuilder = TransactionBegin.newBuilder();
        beginBuilder.setExecuteTime(executeTime);
        return beginBuilder.build();
    }

    public TransactionEnd createTransactionEnd(long transactionId, long executeTime) {
        TransactionEnd.Builder endBuilder = TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(transactionId));
        endBuilder.setExecuteTime(executeTime);
        return endBuilder.build();
    }

    public Pair.Builder createSpecialPair(String key, String value) {
        Pair.Builder pairBuilder = Pair.newBuilder();
        pairBuilder.setKey(key);
        pairBuilder.setValue(value);
        return pairBuilder;
    }

    public Entry createEntry(Header header, EntryType entryType, ByteString storeValue) {
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        return entryBuilder.build();
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setNameFilter(CanalEventFilter<String> nameFilter) {
        this.nameFilter = nameFilter;
    }

    public void setTableMetaCache(TableMetaCache tableMetaCache) {
        this.tableMetaCache = tableMetaCache;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    private Charset getCharset(DBSyncJobConf jobConf, String dbName, String tbName) {
        // use the first match one
        MysqlTableConf mycnf = jobConf.getMysqlTableConf(dbName, tbName);
        if (mycnf == null) {
            return charset;
        }
        Charset taskCharset = mycnf.getCharset();
        if (taskCharset == null) {
            return charset;
        }

        return taskCharset;
    }
}
