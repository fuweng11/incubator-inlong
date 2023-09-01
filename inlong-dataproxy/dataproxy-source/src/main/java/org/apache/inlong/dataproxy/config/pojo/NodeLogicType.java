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

package org.apache.inlong.dataproxy.config.pojo;

import java.util.HashMap;
import java.util.Map;

/**
 * Node running logic type
 */
public enum NodeLogicType {

    INLONG_LOGIC(0, "inlong-logic-with-inlong-meta",
            "Get metadata from InLong and process messages under InLong specification"),
    TDBANK_LOGIC(1, "tdbank-logic-with-tdbank-meta",
            "Get metadata from TDBank and process messages under TDBank specification"),
    TDBANK_LOGIC_WITH_INLONG_META(2, "tdbank-logic-with-inlong-meta",
            "Get metadata from InLOng and process messages under TDBank specification"),
    UNKNOWN(9, "unknown-value", "Unknown value define");

    /**
     *
     * Constructor
     *
     * @param typeId type id
     * @param logicName type name
     * @param desc the logic desc
     */
    private NodeLogicType(int typeId, String logicName, String desc) {
        this.typeId = typeId;
        this.logicName = logicName;
        this.desc = desc;
    }

    public int getTypeId() {
        return typeId;
    }

    public String getDesc() {
        return desc;
    }

    /**
     * get logic name
     *
     * @return the logic name
     */
    public String getLogicName() {
        return this.logicName;
    }

    public static NodeLogicType valueOf(int value) {
        for (NodeLogicType logicType : NodeLogicType.values()) {
            if (logicType.getTypeId() == value) {
                return logicType;
            }
        }
        return UNKNOWN;
    }

    public static Map<Integer, String> getAllowedValues() {
        Map<Integer, String> result = new HashMap<>();
        result.put(INLONG_LOGIC.typeId, INLONG_LOGIC.logicName);
        result.put(TDBANK_LOGIC.typeId, TDBANK_LOGIC.logicName);
        result.put(TDBANK_LOGIC_WITH_INLONG_META.typeId, TDBANK_LOGIC_WITH_INLONG_META.logicName);
        return result;
    }

    public static boolean enableTDBankLogic(NodeLogicType logicType) {
        return !enableInLongLogic(logicType);
    }

    public static boolean isMetaInfoGetFromInLong(NodeLogicType logicType) {
        return !isMetaInfoGetFromTDBank(logicType);
    }

    public static boolean enableInLongLogic(NodeLogicType logicType) {
        return (logicType.typeId == INLONG_LOGIC.typeId);
    }

    public static boolean isMetaInfoGetFromTDBank(NodeLogicType logicType) {
        return (logicType.typeId == TDBANK_LOGIC.typeId);
    }

    private final int typeId;
    private final String logicName;
    private final String desc;
}
