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

package org.apache.inlong.dataproxy.config.holder;

import org.apache.inlong.dataproxy.config.ConfigManager;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link SourceReportConfigHolder}
 */
public class TestDiscardListConfigHolder {

    @Test
    public void testCase() {
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("aaa", "222"));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("aaa", ""));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("bbb", ""));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("bbb", "111"));
        Assert.assertFalse(ConfigManager.getInstance().isDiscardInLongID("CCC", ""));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("CCC", "111"));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("CCC", "333"));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("CCC", "222"));
        Assert.assertTrue(ConfigManager.getInstance().isDiscardInLongID("DDD", "222"));
        Assert.assertFalse(ConfigManager.getInstance().isDiscardInLongID("ddd", "222"));
        Assert.assertFalse(ConfigManager.getInstance().isDiscardInLongID("mmm", "222"));

    }
}
