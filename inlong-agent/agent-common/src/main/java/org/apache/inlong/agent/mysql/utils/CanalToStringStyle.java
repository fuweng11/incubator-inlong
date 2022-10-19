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

package org.apache.inlong.agent.mysql.utils;

import org.apache.commons.lang.builder.ToStringStyle;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CanalToStringStyle extends ToStringStyle {

    private static final long serialVersionUID = -6568177374288222145L;

    private static final String DEFAULT_TIME = "yyyy-MM-dd HH:mm:ss";
    /**
     * <pre>
     * output format:
     * Person[name=John Doe,age=33,smoker=false, time=2010-04-01 00:00:00]
     * </pre>
     */
    public static final ToStringStyle TIME_STYLE = new OtterDateStyle(DEFAULT_TIME);
    /**
     * <pre>
     * output format:
     * Person[name=John Doe,age=33,smoker=false ,time=2010-04-01 00:00:00]
     * </pre>
     */
    public static final ToStringStyle DEFAULT_STYLE = CanalToStringStyle.TIME_STYLE;
    private static final String DEFAULT_DAY = "yyyy-MM-dd";
    /**
     * <pre>
     * output format:
     * Person[name=John Doe,age=33,smoker=false, day=2010-04-01]
     * </pre>
     */
    public static final ToStringStyle DAY_STYLE = new OtterDateStyle(DEFAULT_DAY);

    // =========================== custom style =============================

    private static class OtterDateStyle extends ToStringStyle {

        private static final long serialVersionUID = 5208917932254652886L;

        private String pattern;

        public OtterDateStyle(String pattern) {
            super();
            this.setUseShortClassName(true);
            this.setUseIdentityHashCode(false);
            this.pattern = pattern;
        }

        protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
            if (value instanceof Date) {
                value = new SimpleDateFormat(pattern).format(value);
            }
            buffer.append(value);
        }
    }
}
