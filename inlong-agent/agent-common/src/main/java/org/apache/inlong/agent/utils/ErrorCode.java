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

public class ErrorCode {

    /*
     * db exception
     */
    public static final String DB_EXCEPTION_RELAY_LOG_POSITION = "100000001";
    public static final String DB_EXCEPTION_TABLE_ID_NOT_FOUND = "100000002";
    public static final String DB_EXCEPTION_BINLOG_MISS = "100000003";
    public static final String DB_EXCEPTION_OTHER = "100000004";

    /*
     * op exception
     */
    public static final String OP_EXCEPTION_DUMPER_THREAD_EXIT = "200000001";
    public static final String OP_EXCEPTION_DUMPER_THREAD_EXIT_UNCAUGHT = "200000002";
    public static final String OP_EXCEPTION_DISPATCH_THREAD_EXIT_UNCAUGHT = "200000002";
}
