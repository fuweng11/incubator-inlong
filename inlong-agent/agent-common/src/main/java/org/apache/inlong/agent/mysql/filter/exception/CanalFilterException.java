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

package org.apache.inlong.agent.mysql.filter.exception;

import org.apache.inlong.agent.mysql.connector.exception.CanalException;

public class CanalFilterException extends CanalException {

    private static final long serialVersionUID = -7288830284122672209L;

    public CanalFilterException(String errorCode) {
        super(errorCode);
    }

    public CanalFilterException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public CanalFilterException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public CanalFilterException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalFilterException(Throwable cause) {
        super(cause);
    }

}
