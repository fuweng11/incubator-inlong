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

package org.apache.inlong.agent.mysql.connector.driver.utils;

import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;

public class LengthCodedStringReader {

    public static final String CODE_PAGE_1252 = "Cp1252";

    private String encoding;
    private int index = 0;

    public LengthCodedStringReader(String encoding, int startIndex) {
        this.encoding = encoding;
        this.index = startIndex;
    }

    public String readLengthCodedString(byte[] data) throws IOException {
        byte[] lengthBytes = ByteHelper.readBinaryCodedLengthBytes(data, getIndex());
        long length = ByteHelper.readLengthCodedBinary(data, getIndex());
        setIndex(getIndex() + lengthBytes.length);
        if (ByteHelper.NULL_LENGTH == length) {
            return null;
        }

        try {
            return new String(ArrayUtils.subarray(data, getIndex(), (int) (getIndex() + length)),
                    encoding == null ? CODE_PAGE_1252 : encoding);
        } finally {
            setIndex((int) (getIndex() + length));
        }

    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
