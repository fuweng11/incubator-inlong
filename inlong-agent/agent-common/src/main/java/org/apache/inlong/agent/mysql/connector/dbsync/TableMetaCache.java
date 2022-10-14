package org.apache.inlong.agent.mysql.connector.dbsync;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.mysql.connector.MysqlConnection;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.FieldPacket;
import org.apache.inlong.agent.mysql.connector.driver.packets.server.ResultSetPacket;
import org.apache.inlong.agent.mysql.parse.TableMeta;
import org.apache.inlong.agent.mysql.parse.TableMeta.FieldMeta;
import org.apache.inlong.agent.mysql.connector.exception.CanalParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理table meta解析和缓存
 * 
 * @author lyndldeng 2016-2-25
 * @version 1.0.0
 */
public class TableMetaCache {

    public static final String     COLUMN_NAME    = "COLUMN_NAME";
    public static final String     COLUMN_TYPE    = "COLUMN_TYPE";
    public static final String     IS_NULLABLE    = "IS_NULLABLE";
    public static final String     COLUMN_KEY     = "COLUMN_KEY";
    public static final String     COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String     EXTRA          = "EXTRA";

    public static final String     COLUMN_NAME_MYSQL8    = "Field";
    public static final String     COLUMN_TYPE_MYSQL8    = "Type";
    public static final String     IS_NULLABLE_MYSQL8    = "Null";
    public static final String     COLUMN_KEY_MYSQL8     = "Key";
    public static final String     COLUMN_DEFAULT_MYSQL8 = "Default";
    public static final String     EXTRA_MYSQL8          = "Extra";


    private MysqlConnection connection;
    //private DBSyncJobConf conf;

    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
//    private Map<String, TableMeta> tableMetaCache;
    private LoadingCache<String, TableMeta> tableMetaCache;

    @SuppressWarnings("deprecation")
	public TableMetaCache(MysqlConnection con/*, DBSyncJobConf jobConf*/){
        this.connection = con;
        //this.conf = jobConf;
        this.tableMetaCache = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {
            @Override
            public TableMeta load(String name) throws Exception {
                try {
                    return getTableMeta0(name);
                } catch (Throwable e) {
                    // 尝试做一次retry操作
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
//            tableMetaCache.remove(fullname);
            tableMetaCache.invalidate(fullname);
        }

        return tableMetaCache.getUnchecked(fullname);
    }

    public void clearTableMetaWithFullName(String fullname) {
//        tableMetaCache.remove(fullname);
        tableMetaCache.invalidate(fullname);
    }

    public void clearTableMetaWithSchemaName(String schema) {
        // Set<String> removeNames = new HashSet<String>(); // 存一份临时变量，避免在遍历的时候进行删除
//        for (String name : tableMetaCache.keySet()) {
//            if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
//                // removeNames.add(name);
//                tableMetaCache.remove(name);
//            }
//        }

        for (String name : tableMetaCache.asMap().keySet()) {
            if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                // removeNames.add(name);
                tableMetaCache.invalidate(name);
            }
        }

        // for (String name : removeNames) {
        // tables.remove(name);
        // }
    }

    public void clearTableMeta() {
//        tableMetaCache.clear();
        tableMetaCache.invalidateAll();
    }

    private TableMeta getTableMeta0(String fullname) throws IOException {
        ResultSetPacket packet;
        synchronized (connection) {
            packet = connection.query("desc " + fullname);
        }
        //String[] fullnames = StringUtils.split(fullname, ".");
        //MysqlTableConf tableConf =  conf.getMysqlTableConf(fullnames[0], fullnames[1]);
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
        }
        else {
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
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
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
