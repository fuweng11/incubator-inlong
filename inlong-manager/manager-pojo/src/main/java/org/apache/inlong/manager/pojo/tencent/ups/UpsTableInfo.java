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

package org.apache.inlong.manager.pojo.tencent.ups;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * UPS table info
 */
@Data
public class UpsTableInfo {

    private String code;
    private String message;
    private Object solution;
    private TableInfoBean tableInfo;

    @Data
    public static class TableInfoBean {

        private String dbName;
        private String tableName;
        private String tableType;
        private String format;
        private String location;
        private String inputFormat;
        private String outputFormat;
        private boolean compressed;
        private String priPartKey;
        private Map<String, String> serdeParams;
        private String serdeLib;
        private String priPartType;
        private String subPartKey;
        private String subPartType;
        private String fieldDelimiter;
        private String createTime;
        private String tableOwner;
        private String tableComment;
        private List<ColsInfoBean> colsInfo;
        private List<PartsInfoBean> partsInfo;

        public static class ColsInfoBean {

            private String colName;
            private String colType;
            private String colComment;
            private String colComent;

            public String getColName() {
                return colName;
            }

            public void setColName(String colName) {
                this.colName = colName;
            }

            public String getColType() {
                return colType;
            }

            public void setColType(String colType) {
                this.colType = colType;
            }

            public String getColComment() {
                return colComment;
            }

            public void setColComment(String colComent) {
                this.colComment = colComent;
            }

            public String getColComent() {
                return colComent;
            }

            public void setColComent(String colComent) {
                this.colComent = colComent;
            }
        }

        public static class PartsInfoBean {

            private int level;
            private String partName;
            private Object inputFormat;
            private Object outputFormat;
            private Object serdeLib;
            private String createTime;
            private List<?> partValues;

            public int getLevel() {
                return level;
            }

            public void setLevel(int level) {
                this.level = level;
            }

            public String getPartName() {
                return partName;
            }

            public void setPartName(String partName) {
                this.partName = partName;
            }

            public Object getInputFormat() {
                return inputFormat;
            }

            public void setInputFormat(Object inputFormat) {
                this.inputFormat = inputFormat;
            }

            public Object getOutputFormat() {
                return outputFormat;
            }

            public void setOutputFormat(Object outputFormat) {
                this.outputFormat = outputFormat;
            }

            public Object getSerdeLib() {
                return serdeLib;
            }

            public void setSerdeLib(Object serdeLib) {
                this.serdeLib = serdeLib;
            }

            public String getCreateTime() {
                return createTime;
            }

            public void setCreateTime(String createTime) {
                this.createTime = createTime;
            }

            public List<?> getPartValues() {
                return partValues;
            }

            public void setPartValues(List<?> partValues) {
                this.partValues = partValues;
            }
        }
    }
}
