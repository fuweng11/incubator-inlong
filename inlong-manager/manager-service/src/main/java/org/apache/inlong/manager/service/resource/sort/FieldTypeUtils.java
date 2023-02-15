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

package org.apache.inlong.manager.service.resource.sort;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import java.util.Locale;

public class FieldTypeUtils {

    public static String toStreamFieldType(String type) {
        if (StringUtils.isBlank(type)) {
            throw new BusinessException("type cannot be blank");
        }
        String resultType;
        type = type.toLowerCase(Locale.ROOT);
        switch (type) {
            // bit,tinyint,smallint,mediumint,int,integer,
            case "bit":
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "short":
            case "int":
            case "integer":
                resultType = "int";
                break;

            // bigint,float,double,dec,decimal,
            case "bigint":
            case "long":
                resultType = "long";
                break;
            case "float":
            case "double":
            case "dec":
            case "decimal":
                resultType = "double";
                break;

            // date,time,datetime,timestamp,year,
            case "date":
            case "time":
            case "datetime":
            case "timestamp":
                resultType = "string";
                break;

            // char,varchar,binary,varbinary,blob,text
            case "char":
            case "varchar":
            case "binary":
            case "varbinary":
            case "blob":
            case "text":
                resultType = "string";
                break;

            // enum,set,geometry,point,linestring,polygon,multipoint,multilinestring
            // multipolygon,geometrycollection,json
            default: // 默认是 string
                resultType = "string";
        }

        return resultType;
    }

    public static String toHiveFieldType(String type) {
        if (StringUtils.isBlank(type)) {
            throw new BusinessException("type cannot be blank");
        }
        String resultType;
        type = type.toLowerCase(Locale.ROOT);
        switch (type) {
            // bit,tinyint,smallint,mediumint,int,integer,
            case "bit":
            case "byte":
            case "tinyint":
                resultType = "tinyint";
                break;
            case "smallint":
                resultType = "smallint";
                break;
            case "mediumint":
            case "short":
            case "int":
            case "integer":
                resultType = "int";
                break;

            // bigint,float,double,dec,decimal,
            case "bigint":
            case "long":
                resultType = "bigint";
                break;
            case "float":
            case "double":
            case "dec":
            case "decimal":
            case "boolean":
                resultType = type;
                break;

            // char,varchar,binary,varbinary,blob,text
            case "binary":
            case "varbinary":
                resultType = "binary";
                break;

            // date,time,datetime,timestamp,year,
            case "date":
            case "time":
            case "datetime":
            case "timestamp":
            case "char":
            case "varchar":
            case "blob":
            case "text":
                // enum,set,geometry,point,linestring,polygon,multipoint,multilinestring
                // multipolygon,geometrycollection,json
            default: // 默认是 string
                resultType = "string";
        }

        return resultType;
    }
}
