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

package org.apache.inlong.agent.utils;

import org.apache.inlong.agent.mysql.connector.binlog.LogBuffer;
import org.apache.inlong.agent.mysql.connector.binlog.LogEvent;

public class JsonParser {

    /**
     * handle MySQL, convert json to string
     */
    // JSON TYPE
    public static final int JSONB_TYPE_SMALL_OBJECT = 0x0;
    public static final int JSONB_TYPE_LARGE_OBJECT = 0x1;
    public static final int JSONB_TYPE_SMALL_ARRAY = 0x2;
    public static final int JSONB_TYPE_LARGE_ARRAY = 0x3;
    public static final int JSONB_TYPE_LITERAL = 0x4;
    public static final int JSONB_TYPE_INT16 = 0x5;
    public static final int JSONB_TYPE_UINT16 = 0x6;
    public static final int JSONB_TYPE_INT32 = 0x7;
    public static final int JSONB_TYPE_UINT32 = 0x8;
    public static final int JSONB_TYPE_INT64 = 0x9;
    public static final int JSONB_TYPE_UINT64 = 0xA;
    public static final int JSONB_TYPE_DOUBLE = 0xB;
    public static final int JSONB_TYPE_STRING = 0xC;
    public static final int JSONB_TYPE_OPAQUE = 0xF;
    public static final char JSONB_NULL_LITERAL = '\0';
    public static final char JSONB_TRUE_LITERAL = '\1';
    public static final char JSONB_FALSE_LITERAL = '\2';

    /*
     * The size of offset or size fields in the small and the large storage format for JSON objects and JSON arrays.
     */
    public static final int SMALL_OFFSET_SIZE = 2;
    public static final int LARGE_OFFSET_SIZE = 4;

    /*
     * The size of key entries for objects when using the small storage format or the large storage format. In the small
     * format it is 4 bytes (2 bytes for key length and 2 bytes for key offset). In the large format it is 6 (2 bytes
     * for length, 4 bytes for offset).
     */
    public static final int KEY_ENTRY_SIZE_SMALL = (2 + SMALL_OFFSET_SIZE);
    public static final int KEY_ENTRY_SIZE_LARGE = (2 + LARGE_OFFSET_SIZE);

    /*
     * The size of value entries for objects or arrays. When using the small storage format, the entry size is 3 (1 byte
     * for type, 2 bytes for offset). When using the large storage format, it is 5 (1 byte for type, 4 bytes for
     * offset).
     */
    public static final int VALUE_ENTRY_SIZE_SMALL = (1 + SMALL_OFFSET_SIZE);
    public static final int VALUE_ENTRY_SIZE_LARGE = (1 + LARGE_OFFSET_SIZE);

    public static JsonValue parse_value(int type, LogBuffer buffer, long len) {
        buffer = buffer.duplicate(buffer.position(), (int) len);
        switch (type) {
            case JSONB_TYPE_SMALL_OBJECT:
                return parse_array_or_object(JsonEnumType.OBJECT, buffer, len, false);
            case JSONB_TYPE_LARGE_OBJECT:
                return parse_array_or_object(JsonEnumType.OBJECT, buffer, len, true);
            case JSONB_TYPE_SMALL_ARRAY:
                return parse_array_or_object(JsonEnumType.ARRAY, buffer, len, false);
            case JSONB_TYPE_LARGE_ARRAY:
                return parse_array_or_object(JsonEnumType.ARRAY, buffer, len, true);
            default:
                return parse_scalar(type, buffer, len);
        }
    }

    private static JsonValue parse_array_or_object(JsonEnumType type, LogBuffer buffer, long len, boolean large) {
        long offsetSize = large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
        if (len < 2 * offsetSize) {
            throw new IllegalArgumentException("illegal json data");
        }
        long elementCount = read_offset_or_size(buffer, large);
        long bytes = read_offset_or_size(buffer, large);

        if (bytes > len) {
            throw new IllegalArgumentException("illegal json data");
        }
        long headerSize = 2 * offsetSize;
        if (type == JsonEnumType.OBJECT) {
            headerSize += elementCount * (large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);
        }

        headerSize += elementCount * (large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL);
        if (headerSize > bytes) {
            throw new IllegalArgumentException("illegal json data");
        }
        return new JsonValue(type, buffer.rewind(), elementCount, bytes, large);
    }

    private static long read_offset_or_size(LogBuffer buffer, boolean large) {
        return large ? buffer.getUint32() : buffer.getUint16();
    }

    private static JsonValue parse_scalar(int type, LogBuffer buffer, long len) {
        switch (type) {
            case JSONB_TYPE_LITERAL:
                /* purecov: inspected */
                int data = buffer.getUint8();
                switch (data) {
                    case JSONB_NULL_LITERAL:
                        return new JsonValue(JsonEnumType.LITERAL_NULL);
                    case JSONB_TRUE_LITERAL:
                        return new JsonValue(JsonEnumType.LITERAL_TRUE);
                    case JSONB_FALSE_LITERAL:
                        return new JsonValue(JsonEnumType.LITERAL_FALSE);
                    default:
                        throw new IllegalArgumentException("illegal json data");
                }
            case JSONB_TYPE_INT16:
                return new JsonValue(JsonEnumType.INT, buffer.getInt16());
            case JSONB_TYPE_INT32:
                return new JsonValue(JsonEnumType.INT, buffer.getInt32());
            case JSONB_TYPE_INT64:
                return new JsonValue(JsonEnumType.INT, buffer.getLong64());
            case JSONB_TYPE_UINT16:
                return new JsonValue(JsonEnumType.UINT, buffer.getUint16());
            case JSONB_TYPE_UINT32:
                return new JsonValue(JsonEnumType.UINT, buffer.getUint32());
            case JSONB_TYPE_UINT64:
                return new JsonValue(JsonEnumType.UINT, buffer.getUlong64());
            case JSONB_TYPE_DOUBLE:
                return new JsonValue(JsonEnumType.DOUBLE, Double.valueOf(buffer.getDouble64()));
            case JSONB_TYPE_STRING:
                int maxBytes = (int) Math.min(len, 5);
                long tlen = 0;
                long strLen = 0;
                long n = 0;
                byte[] datas = buffer.getData(maxBytes);
                for (int i = 0; i < maxBytes; i++) {
                    // Get the next 7 bits of the length.
                    tlen |= (datas[i] & 0x7f) << (7 * i);
                    if ((datas[i] & 0x80) == 0) {
                        // The length shouldn't exceed 32 bits.
                        if (tlen > 4294967296L) {
                            throw new IllegalArgumentException("illegal json data");
                        }

                        // This was the last byte. Return successfully.
                        n = i + 1;
                        strLen = tlen;
                        break;
                    }
                }

                if (len < n + strLen) {
                    throw new IllegalArgumentException("illegal json data");
                }
                return new JsonValue(JsonEnumType.STRING, buffer.rewind()
                        .forward((int) n).getFixString((int) strLen));
            case JSONB_TYPE_OPAQUE:
                /*
                 * There should always be at least one byte, which tells the field type of the opaque value.
                 */
                // The type is encoded as a uint8 that maps to an
                // enum_field_types.
                int typeByte = buffer.getUint8();
                int position = buffer.position();
                // Then there's the length of the value.
                int qMaxBytes = (int) Math.min(len, 5);
                long qTlen = 0;
                long qStrLen = 0;
                long qN = 0;
                byte[] qDatas = buffer.getData(qMaxBytes);
                for (int i = 0; i < qMaxBytes; i++) {
                    // Get the next 7 bits of the length.
                    qTlen |= (qDatas[i] & 0x7f) << (7 * i);
                    if ((qDatas[i] & 0x80) == 0) {
                        // The length shouldn't exceed 32 bits.
                        if (qTlen > 4294967296L) {
                            throw new IllegalArgumentException("illegal json data");
                        }

                        // This was the last byte. Return successfully.
                        qN = i + 1;
                        qStrLen = qTlen;
                        break;
                    }
                }

                if (qStrLen == 0 || len < qN + qStrLen) {
                    throw new IllegalArgumentException("illegal json data");
                }
                return new JsonValue(typeByte, buffer.position(position).forward((int) qN), qStrLen);
            default:
                throw new IllegalArgumentException("illegal json data");
        }
    }

    private static String usecondsToStr(int frac, int meta) {
        String sec = String.valueOf(frac);
        if (meta > 6) {
            throw new IllegalArgumentException("unknow useconds meta : " + meta);
        }

        if (sec.length() < 6) {
            StringBuilder result = new StringBuilder(6);
            int len = 6 - sec.length();
            for (; len > 0; len--) {
                result.append('0');
            }
            result.append(sec);
            sec = result.toString();
        }

        return sec.substring(0, meta);
    }

    public static enum JsonEnumType {
        OBJECT, ARRAY, STRING, INT, UINT, DOUBLE, LITERAL_NULL, LITERAL_TRUE, LITERAL_FALSE, OPAQUE, ERROR
    }

    public static class JsonValue {

        JsonEnumType mType;
        int mFieldType;
        LogBuffer mData;
        long mElementCount;
        long mLength;
        String mStringValue;
        Number mIntValue;
        double mDoubleValue;
        boolean mLarge;

        public JsonValue(JsonEnumType t) {
            this.mType = t;
        }

        public JsonValue(JsonEnumType t, Number val) {
            this.mType = t;
            if (t == JsonEnumType.DOUBLE) {
                this.mDoubleValue = val.doubleValue();
            } else {
                this.mIntValue = val;
            }
        }

        public JsonValue(JsonEnumType t, String value) {
            this.mType = t;
            this.mStringValue = value;
        }

        public JsonValue(int fieldType, LogBuffer data, long bytes) {
            this.mType = JsonEnumType.OPAQUE; // uncertain type
            this.mFieldType = fieldType;
            this.mData = data;
            this.mLength = bytes;
        }

        public JsonValue(JsonEnumType t, LogBuffer data, long elementCount, long bytes, boolean large) {
            this.mType = t;
            this.mData = data;
            this.mElementCount = elementCount;
            this.mLength = bytes;
            this.mLarge = large;
        }

        public String key(int i) {
            mData.rewind();
            int offsetSize = mLarge ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
            int keyEntrySize = mLarge ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
            int entryOffset = 2 * offsetSize + keyEntrySize * i;
            // The offset of the key is the first part of the key
            // entry.
            mData.forward(entryOffset);
            long keyOffset = read_offset_or_size(mData, mLarge);
            // The length of the key is the second part of the
            // entry, always two
            // bytes.
            long keyLength = mData.getUint16();
            return mData.rewind().forward((int) keyOffset).getFixString((int) keyLength);
        }

        public JsonValue element(int i) {
            mData.rewind();
            int offsetSize = mLarge ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
            int keyEntrySize = mLarge ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
            int valueEntrySize = mLarge ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
            int firstEntryOffset = 2 * offsetSize;
            if (mType == JsonEnumType.OBJECT) {
                firstEntryOffset += mElementCount * keyEntrySize;
            }
            int entryOffset = firstEntryOffset + valueEntrySize * i;
            int type = mData.forward(entryOffset).getUint8();
            if (type == JSONB_TYPE_INT16 || type == JSONB_TYPE_UINT16 || type == JSONB_TYPE_LITERAL
                    || (mLarge && (type == JSONB_TYPE_INT32 || type == JSONB_TYPE_UINT32))) {
                return parse_scalar(type, mData, valueEntrySize - 1);
            }
            int valueOffset = (int) read_offset_or_size(mData, mLarge);
            return parse_value(type, mData.rewind().forward(valueOffset), (int) mLength - valueOffset);
        }

        public StringBuilder toJsonString(StringBuilder buf) {
            switch (mType) {
                case OBJECT:
                    buf.append("{");
                    for (int i = 0; i < mElementCount; ++i) {
                        if (i > 0) {
                            buf.append(", ");
                        }
                        buf.append('"').append(key(i)).append('"');
                        buf.append(": ");
                        element(i).toJsonString(buf);
                    }
                    buf.append("}");
                    break;
                case ARRAY:
                    buf.append("[");
                    for (int i = 0; i < mElementCount; ++i) {
                        if (i > 0) {
                            buf.append(", ");
                        }
                        element(i).toJsonString(buf);
                    }
                    buf.append("]");
                    break;
                case DOUBLE:
                    buf.append(Double.valueOf(mDoubleValue).toString());
                    break;
                case INT:
                    buf.append(mIntValue.toString());
                    break;
                case UINT:
                    buf.append(mIntValue.toString());
                    break;
                case LITERAL_FALSE:
                    buf.append("false");
                    break;
                case LITERAL_TRUE:
                    buf.append("true");
                    break;
                case LITERAL_NULL:
                    buf.append("NULL");
                    break;
                case OPAQUE:
                    String text = null;
                    if (mFieldType == LogEvent.MYSQL_TYPE_NEWDECIMAL) {
                        int precision = mData.getInt8();
                        int scale = mData.getInt8();
                        text = mData.getDecimal(precision, scale).toPlainString();
                        buf.append(text);
                    } else if (mFieldType == LogEvent.MYSQL_TYPE_TIME) {
                        long packedValue = mData.getLong64();
                        if (packedValue == 0) {
                            text = "00:00:00";
                        } else {
                            long ultime = Math.abs(packedValue);
                            long intpart = ultime >> 24;
                            int frac = (int) (ultime % (1L << 24));
                            text = String.format("%s%02d:%02d:%02d",
                                    packedValue >= 0 ? "" : "-",
                                    (int) ((intpart >> 12) % (1 << 10)),
                                    (int) ((intpart >> 6) % (1 << 6)),
                                    (int) (intpart % (1 << 6)));
                            text = text + "." + usecondsToStr(frac, 6);
                        }
                        buf.append('"').append(text).append('"');
                    } else if (mFieldType == LogEvent.MYSQL_TYPE_DATE || mFieldType == LogEvent.MYSQL_TYPE_DATETIME
                            || mFieldType == LogEvent.MYSQL_TYPE_TIMESTAMP) {
                        long packedValue = mData.getLong64();
                        if (packedValue == 0) {
                            text = "0000-00-00 00:00:00";
                        } else {
                            // second timestamp
                            long ultime = Math.abs(packedValue);
                            long intpart = ultime >> 24;
                            int frac = (int) (ultime % (1L << 24));
                            long ymd = intpart >> 17;
                            long ym = ymd >> 5;
                            long hms = intpart % (1 << 17);
                            text = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                    (int) (ym / 13),
                                    (int) (ym % 13),
                                    (int) (ymd % (1 << 5)),
                                    (int) (hms >> 12),
                                    (int) ((hms >> 6) % (1 << 6)),
                                    (int) (hms % (1 << 6)));
                            text = text + "." + usecondsToStr(frac, 6);
                        }
                        buf.append('"').append(text).append('"');
                    } else {
                        text = mData.getFixString((int) mLength);
                        buf.append('"').append(text).append('"');
                    }

                    break;
                case STRING:
                    buf.append('"').append(mStringValue).append('"');
                    break;
                case ERROR:
                    throw new IllegalArgumentException("illegal json data");
            }

            return buf;
        }
    }

}
