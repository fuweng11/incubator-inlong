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

package org.apache.inlong.agent.mysql.filter.aviater;

import org.apache.inlong.agent.common.protocol.CanalEntry;
import org.apache.inlong.agent.common.protocol.CanalEntry.Entry;
import org.apache.inlong.agent.mysql.filter.CanalEventFilter;
import org.apache.inlong.agent.mysql.filter.exception.CanalFilterException;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class AviaterELFilter implements CanalEventFilter<Entry> {

    public static final String ROOT_KEY = "entry";
    private String expression;

    public AviaterELFilter(String expression) {
        this.expression = expression;
    }

    public boolean filter(CanalEntry.Entry entry) throws CanalFilterException {
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put(ROOT_KEY, entry);
        return (Boolean) AviatorEvaluator.execute(expression, env);
    }

}
