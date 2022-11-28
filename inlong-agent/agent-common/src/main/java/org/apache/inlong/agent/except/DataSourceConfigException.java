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

package org.apache.inlong.agent.except;

public class DataSourceConfigException extends Exception {

    public DataSourceConfigException(String msg) {
        super(msg);
    }

    public DataSourceConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSourceConfigException(Throwable cause) {
        super(cause);
    }

    public DataSourceConfigException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DataSourceConfigException() {
    }

    public static class JobNotFoundException extends DataSourceConfigException {

        public JobNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class InvalidCharsetNameException extends DataSourceConfigException {

        public InvalidCharsetNameException(String msg) {
            super(msg);
        }
    }

    public static class JobSizeExceedMaxException extends DataSourceConfigException {

        public JobSizeExceedMaxException(String msg) {
            super(msg);
        }
    }
}
