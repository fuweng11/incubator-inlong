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

package org.apache.inlong.agent.mysql.connector.driver.packets.server;

import org.apache.inlong.agent.mysql.connector.driver.packets.PacketWithHeaderPacket;
import org.apache.inlong.agent.mysql.connector.driver.utils.ByteHelper;
import org.apache.inlong.agent.mysql.connector.driver.utils.LengthCodedStringReader;

import java.io.IOException;

public class FieldPacket extends PacketWithHeaderPacket {

    private String catalog;
    private String db;
    private String table;
    private String originalTable;
    private String name;
    private String originalName;
    private int    character;
    private long   length;
    private byte   type;
    private int    flags;
    private byte   decimals;
    private String definition;

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                      Name
     *  -----                      ----
     *  n (Length Coded String)    catalog
     *  n (Length Coded String)    db
     *  n (Length Coded String)    table
     *  n (Length Coded String)    org_table
     *  n (Length Coded String)    name
     *  n (Length Coded String)    org_name
     *  1                          (filler)
     *  2                          charsetnr
     *  4                          length
     *  1                          type
     *  2                          flags
     *  1                          decimals
     *  2                          (filler), always 0x00
     *  n (Length Coded Binary)    default
     * 
     * </pre>
     */
    public void fromBytes(byte[] data) throws IOException {

        int index = 0;
        LengthCodedStringReader reader = new LengthCodedStringReader(null, index);
        // 1.
        catalog = reader.readLengthCodedString(data);
        // 2.
        db = reader.readLengthCodedString(data);
        this.table = reader.readLengthCodedString(data);
        this.originalTable = reader.readLengthCodedString(data);
        this.name = reader.readLengthCodedString(data);
        this.originalName = reader.readLengthCodedString(data);
        index = reader.getIndex();
        // 
        index++;
        // 
        this.character = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        //
        this.length = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index += 4;
        // 
        this.type = data[index];
        index++;
        // 
        this.flags = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 
        this.decimals = data[index];
        index++;
        // 
        if (index < data.length) {
            reader.setIndex(index);
            this.definition = reader.readLengthCodedString(data);
        }
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOriginalTable() {
        return originalTable;
    }

    public void setOriginalTable(String originalTable) {
        this.originalTable = originalTable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOriginalName() {
        return originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public int getCharacter() {
        return character;
    }

    public void setCharacter(int character) {
        this.character = character;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public byte getDecimals() {
        return decimals;
    }

    public void setDecimals(byte decimals) {
        this.decimals = decimals;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public String toString() {
        return "FieldPacket [catalog=" + catalog + ", character=" + character + ", db=" + db + ", decimals=" + decimals
               + ", definition=" + definition + ", flags=" + flags + ", length=" + length + ", name=" + name
               + ", originalName=" + originalName + ", originalTable=" + originalTable + ", table=" + table + ", type="
               + type + "]";
    }

}
