package org.apache.inlong.agent.mysql.parse;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.mysql.connector.binlog.event.TableMapLogEvent;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * 描述数据meta对象,mysql binlog中对应的{@linkplain TableMapLogEvent}包含的信息不全
 * 
 * <pre>
 * 1. 主键信息
 * 2. column name
 * 3. unsigned字段
 * </pre>
 * 
 * @author jianghang 2013-1-18 下午12:24:59
 * @version 1.0.0
 */
public class TableMeta {

	protected final Logger logger = LogManager.getLogger(this.getClass());
	
    private String          fullName; // schema.table
    private List<FieldMeta> fields;
    //for Gen key
    //private List<Integer> keyList;
    //private HashSet<Integer> needFields;    

	//MysqlTableConf tableConf;

	public TableMeta(String fullName, List<FieldMeta> fields/*, MysqlTableConf tableConf*/){
        this.fullName = fullName;
        this.fields = fields;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public List<FieldMeta> getFileds() {
        return fields;
    }

    public void setFileds(List<FieldMeta> fileds) {
        this.fields = fileds;
    }

    public static class FieldMeta {

        private String columnName;
        private String columnType;
        private String isNullable;
        private String iskey;
        private String defaultValue;
        private String extra;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        public String getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(String isNullable) {
            this.isNullable = isNullable;
        }

        public String getIskey() {
            return iskey;
        }

        public void setIskey(String iskey) {
            this.iskey = iskey;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public boolean isUnsigned() {
            return StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        public boolean isKey() {
            return StringUtils.equalsIgnoreCase(iskey, "PRI");
        }

        public boolean isNullable() {
            return StringUtils.equalsIgnoreCase(isNullable, "YES");
        }

        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", defaultValue="
                   + defaultValue + ", extra=" + extra + ", isNullable=" + isNullable + ", iskey=" + iskey + "]";
        }

    }
}
