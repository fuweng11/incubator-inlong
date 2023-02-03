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

package org.apache.inlong.sort.base.dirty;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class RegexReplaceTest {

    @Test
    public void testRegexReplacement() throws IOException {
        String[] identifier = new String[2];
        identifier[0] = "yizhouyang";
        identifier[1] = "table2";
        String pattern = "${database}-${table}-${DIRTY_MESSAGE}";
        String answer = DirtySinkHelper.regexReplace(pattern, DirtyType.BATCH_LOAD_ERROR, "mock message", identifier);
        Assert.assertEquals("yizhouyang-table2-mock message", answer);
    }
}
