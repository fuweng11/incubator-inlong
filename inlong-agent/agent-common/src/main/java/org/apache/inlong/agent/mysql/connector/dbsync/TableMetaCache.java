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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.FieldPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;
import org.apache.inlong.agent.mysql.parse.TableMeta;
import org.apache.inlong.agent.mysql.parse.TableMeta.FieldMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetaCache {

    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String COLUMN_TYPE = "COLUMN_TYPE";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String COLUMN_KEY = "COLUMN_KEY";
    public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String EXTRA = "EXTRA";

    public static final String COLUMN_NAME_MYSQL8 = "Field";
    public static final String COLUMN_TYPE_MYSQL8 = "Type";
    public static final String IS_NULLABLE_MYSQL8 = "Null";
    public static final String COLUMN_KEY_MYSQL8 = "Key";
    public static final String COLUMN_DEFAULT_MYSQL8 = "Default";
    public static final String EXTRA_MYSQL8 = "Extra";


    private MysqlConnection connection;
    private LoadingCache<String, TableMeta> tableMetaCache;

    @SuppressWarnings("deprecation")
    public TableMetaCache(MysqlConnection con/*, DBSyncJobConf jobConf*/) {
        this.connection = con;
        //this.conf = jobConf;
        this.tableMetaCache = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {
            @Override
            public TableMeta load(String name) throws Exception {
                try {
                    return getTableMeta0(name);
                } catch (Throwable e) {
                    //retry operation
                    try {
                        synchronized (connection) {
                            connection.reconnect();
                        }
                        return getTableMeta0(name);
                    } catch (IOException e1) {
                        throw new CanalParseException("fetch failed by table meta:" + name, e1);
                    }
                }
            }

        });

    }

    public TableMeta getTableMeta(String fullname) {
        return getTableMeta(fullname, true);
    }

    public TableMeta getTableMeta(String fullname, boolean useCache) {
        if (!useCache) {
            tableMetaCache.invalidate(fullname);
        }

        return tableMetaCache.getUnchecked(fullname);
    }

    public void clearTableMetaWithFullName(String fullname) {
        tableMetaCache.invalidate(fullname);
    }

    public void clearTableMetaWithSchemaName(String schema) {

        for (String name : tableMetaCache.asMap().keySet()) {
            if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                tableMetaCache.invalidate(name);
            }
        }
    }

    public void clearTableMeta() {
        tableMetaCache.invalidateAll();
    }

    private TableMeta getTableMeta0(String fullname) throws IOException {
        ResultSetPacket packet;
        synchronized (connection) {
            packet = connection.query("desc " + fullname);
        }
        return new TableMeta(fullname, parserTableMeta(packet)/*, tableConf*/);
    }

    private List<FieldMeta> parserTableMeta(ResultSetPacket packet) {
        Map<String, Integer> nameMaps = new HashMap<String, Integer>(6, 1f);

        int index = 0;
        for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
            nameMaps.put(fieldPacket.getOriginalName(), index++);
        }

        int size = packet.getFieldDescriptors().size();
        int count = packet.getFieldValues().size() / packet.getFieldDescriptors().size();
        List<FieldMeta> result = new ArrayList<FieldMeta>();

        if (nameMaps.get(COLUMN_NAME) == null) {
            // deal with mysql 8.0
            extractMeta(packet, nameMaps, size, count, result, COLUMN_NAME_MYSQL8,
                    COLUMN_TYPE_MYSQL8,
                    IS_NULLABLE_MYSQL8, COLUMN_KEY_MYSQL8, COLUMN_DEFAULT_MYSQL8, EXTRA_MYSQL8);
        } else {
            extractMeta(packet, nameMaps, size, count, result, COLUMN_NAME, COLUMN_TYPE,
                    IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA);
        }

        return result;
    }

    private void extractMeta(ResultSetPacket packet, Map<String, Integer> nameMaps, int size,
            int count, List<FieldMeta> result, String columnName, String columnType,
            String isNullable, String columnKey, String columnDefault,
            String extraMysql) {
        for (int i = 0; i < count; i++) {
            FieldMeta meta = new FieldMeta();
            meta.setColumnName(
                    packet.getFieldValues().get(nameMaps.get(columnName) + i * size).intern());
            meta.setColumnType(
                    packet.getFieldValues().get(nameMaps.get(columnType) + i * size));
            meta.setIsNullable(
                    packet.getFieldValues().get(nameMaps.get(isNullable) + i * size));
            meta.setIskey(packet.getFieldValues().get(nameMaps.get(columnKey) + i * size));
            meta.setDefaultValue(
                    packet.getFieldValues().get(nameMaps.get(columnDefault) + i * size));
            meta.setExtra(packet.getFieldValues().get(nameMaps.get(extraMysql) + i * size));
            result.add(meta);
        }
    }

}
