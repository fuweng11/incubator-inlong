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

package org.apache.inlong.agent.conf;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDBSyncJobConf {

    private DBSyncJobConf conf;

    @Before
    public void setUp() throws Exception {
        conf = new DBSyncJobConf("127.0.0.1", 3306, "test", "test", StandardCharsets.UTF_8, null, "serverId");
    }

    @Test
    public void testRegexTable() {
        MysqlTableConf tableConf1 = new MysqlTableConf("jobName", "inlongtest", "student.*", "groupId", "streamId", 29,
                Charset.defaultCharset(), true);
        conf.addTable(tableConf1);
        MysqlTableConf tableConf2 = new MysqlTableConf("jobName", "inlongtest", "student_1", "groupId", "streamId", 30,
                Charset.defaultCharset(), true);
        conf.addTable(tableConf2);
        String dbName = "inlongtest";
        String tableName = "student_1";
        conf.getFilter().filter(dbName + "." + tableName);
        assertTrue(conf.bInNeedTable(dbName, tableName));
        List<MysqlTableConf> mysqlTableConfList = conf
                .getMysqlTableConfList(dbName, tableName);
        assertEquals(2, mysqlTableConfList.size());
    }

}
