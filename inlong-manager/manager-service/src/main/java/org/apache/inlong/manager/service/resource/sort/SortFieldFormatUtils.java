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

import com.tencent.flink.formats.common.ArrayFormatInfo;
import com.tencent.flink.formats.common.BooleanFormatInfo;
import com.tencent.flink.formats.common.ByteFormatInfo;
import com.tencent.flink.formats.common.ByteTypeInfo;
import com.tencent.flink.formats.common.DateFormatInfo;
import com.tencent.flink.formats.common.DecimalFormatInfo;
import com.tencent.flink.formats.common.DoubleFormatInfo;
import com.tencent.flink.formats.common.FloatFormatInfo;
import com.tencent.flink.formats.common.FormatInfo;
import com.tencent.flink.formats.common.IntFormatInfo;
import com.tencent.flink.formats.common.LongFormatInfo;
import com.tencent.flink.formats.common.ShortFormatInfo;
import com.tencent.flink.formats.common.StringFormatInfo;
import com.tencent.flink.formats.common.TimeFormatInfo;
import com.tencent.flink.formats.common.TimestampFormatInfo;

/**
 * Sort field formatting tool
 */
public class SortFieldFormatUtils {

    /**
     * The field type is converted to the type in sort
     *
     * @see <a href="https://iwiki.woa.com/pages/viewpage.action?pageId=882632233">Mapping relationship between ETL built-in type and sink type</a>
     */
    public static FormatInfo convertFieldFormat(String type) {
        FormatInfo formatInfo;
        switch (type) {
            case "boolean":
                formatInfo = new BooleanFormatInfo();
                break;
            case "int8":
            case "tinyint":
            case "byte":
                formatInfo = new ByteFormatInfo();
                break;
            case "smallint":
            case "short":
            case "int16":
                formatInfo = new ShortFormatInfo();
                break;
            case "int":
            case "int32":
            case "integer":
                formatInfo = new IntFormatInfo();
                break;
            case "bigint":
            case "long":
            case "int64":
                formatInfo = new LongFormatInfo();
                break;
            case "float":
            case "float32":
                formatInfo = new FloatFormatInfo();
                break;
            case "double":
            case "float64":
                formatInfo = new DoubleFormatInfo();
                break;
            case "decimal":
                formatInfo = new DecimalFormatInfo();
                break;
            case "date":
                formatInfo = new DateFormatInfo();
                break;
            case "time":
                formatInfo = new TimeFormatInfo();
                break;
            case "timestamp":
                formatInfo = new TimestampFormatInfo();
                break;
            case "binary":
            case "fixed":
                formatInfo = new ArrayFormatInfo(ByteTypeInfo::new);
                break;
            default: // The default is string
                formatInfo = new StringFormatInfo();
        }

        return formatInfo;
    }

    public static FormatInfo convertFieldFormat(String type, String format) {
        FormatInfo formatInfo;
        switch (type) {
            case "boolean":
                formatInfo = new BooleanFormatInfo();
                break;
            case "tinyint":
            case "byte":
                formatInfo = new ByteFormatInfo();
                break;
            case "smallint":
            case "short":
                formatInfo = new ShortFormatInfo();
                break;
            case "int":
                formatInfo = new IntFormatInfo();
                break;
            case "bigint":
            case "long":
                formatInfo = new LongFormatInfo();
                break;
            case "float":
                formatInfo = new FloatFormatInfo();
                break;
            case "double":
                formatInfo = new DoubleFormatInfo();
                break;
            case "decimal":
                formatInfo = new DecimalFormatInfo();
                break;
            case "date":
                formatInfo = new DateFormatInfo(format);
                break;
            case "time":
                formatInfo = new TimeFormatInfo(format);
                break;
            case "timestamp":
                formatInfo = new TimestampFormatInfo(format);
                break;
            case "binary":
            case "fixed":
                formatInfo = new ArrayFormatInfo(ByteTypeInfo::new);
                break;
            default: // The default is string
                formatInfo = new StringFormatInfo();
        }

        return formatInfo;
    }

}
