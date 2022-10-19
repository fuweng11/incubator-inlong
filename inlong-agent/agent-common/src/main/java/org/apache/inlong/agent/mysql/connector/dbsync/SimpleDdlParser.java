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

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.protocol.DBSyncMsg.EventType;
import org.apache.inlong.agent.mysql.filter.PatternUtils;
import org.apache.oro.text.regex.Perl5Matcher;

public class SimpleDdlParser {

    public static final String CREATE_PATTERN = "^\\s*CREATE\\s*TABLE\\s*(.*)$";
    public static final String DROP_PATTERN = "^\\s*DROP\\s*TABLE\\s*(.*)$";
    public static final String ALERT_PATTERN = "^\\s*ALTER\\s*TABLE\\s*(.*)$";
    public static final String TABLE_PATTERN = "^(IF\\s*NOT\\s*EXIST\\s*)?(IF\\s*"
            + "EXIST\\s*)?(`?.+?`?\\.)?(`?.+?`?[;\\(\\s]+?)?.*$";

    public static DdlResult parse(String queryString, String schmeaName) {
        DdlResult result = parse(queryString, schmeaName, ALERT_PATTERN);
        if (result != null) {
            result.setType(EventType.ALTER);
            return result;
        }

        result = parse(queryString, schmeaName, CREATE_PATTERN);
        if (result != null) {
            result.setType(EventType.CREATE);
            return result;
        }

        result = parse(queryString, schmeaName, DROP_PATTERN);
        if (result != null) {
            result.setType(EventType.ERASE);
            return result;
        }

        result = new DdlResult(schmeaName);
        result.setType(EventType.QUERY);
        return result;
    }

    private static DdlResult parse(String queryString, String schmeaName, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            Perl5Matcher tableMatcher = new Perl5Matcher();
            String matchString = matcher.getMatch().group(1) + " ";
            if (tableMatcher.matches(matchString, PatternUtils.getPattern(TABLE_PATTERN))) {
                String schmeaString = tableMatcher.getMatch().group(3);
                String tableString = tableMatcher.getMatch().group(4);
                if (StringUtils.isNotEmpty(schmeaString)) {
                    // handle `
                    schmeaString = StringUtils.removeEnd(schmeaString, ".");
                    schmeaString = StringUtils.removeEnd(schmeaString, "`");
                    schmeaString = StringUtils.removeStart(schmeaString, "`");

                    if (StringUtils.isNotEmpty(schmeaName) && !StringUtils.equalsIgnoreCase(schmeaString, schmeaName)) {
                        return new DdlResult(schmeaName);
                    }
                } else {
                    schmeaString = schmeaName;
                }

                tableString = StringUtils.removeEnd(tableString, ";");
                tableString = StringUtils.removeEnd(tableString, "(");
                tableString = StringUtils.trim(tableString);
                // handle `
                tableString = StringUtils.removeEnd(tableString, "`");
                tableString = StringUtils.removeStart(tableString, "`");
                // handle schema.table
                String[] names = StringUtils.split(tableString, ".");
                if (names != null && names.length > 1) {
                    if (StringUtils.equalsIgnoreCase(schmeaString, names[0])) {
                        return new DdlResult(schmeaString, names[1]);
                    }
                } else {
                    return new DdlResult(schmeaString, names[0]);
                }
            }

            return new DdlResult(schmeaName);
        }

        return null;
    }

    public static class DdlResult {

        private String schemaName;
        private String tableName;
        private EventType type;

        public DdlResult() {
        }

        public DdlResult(String schemaName) {
            this.schemaName = schemaName;
        }

        public DdlResult(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public EventType getType() {
            return type;
        }

        public void setType(EventType type) {
            this.type = type;
        }

        public String toString() {
            return "DdlResult [schemaName=" + schemaName + ", tableName=" + tableName + ", type=" + type + "]";
        }

    }
}
