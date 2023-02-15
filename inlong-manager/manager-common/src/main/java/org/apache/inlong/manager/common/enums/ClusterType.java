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

package org.apache.inlong.manager.common.enums;

import org.apache.inlong.manager.common.exceptions.BusinessException;

import java.util.HashSet;
import java.util.Set;

/**
 * Constant of cluster type.
 */
public class ClusterType {

    public static final String AGENT = "AGENT";
    public static final String TUBEMQ = "TUBEMQ";
    public static final String PULSAR = "PULSAR";
    public static final String DATAPROXY = "DATAPROXY";
    public static final String KAFKA = "KAFKA";
    public static final String ELASTICSEARCH = "ELASTICSEARCH";

    // --------------------------------------------------------------------------------------------
    // Inner parameters
    // --------------------------------------------------------------------------------------------
    public static final String ZOOKEEPER = "ZOOKEEPER";
    public static final String DBSYNC_ZK = "DBSYNC_ZK";
    public static final String SORT_HIVE = "SORT_HIVE";
    public static final String SORT_THIVE = "SORT_THIVE";
    public static final String SORT_CK = "SORT_CK";
    public static final String SORT_ICEBERG = "SORT_ICEBERG";
    public static final String SORT_ES = "SORT_ES";
    public static final String CUSTOM = "CUSTOM";

    private static final Set<String> TYPE_SET = new HashSet<String>() {

        {
            add(ClusterType.AGENT);
            add(ClusterType.TUBEMQ);
            add(ClusterType.PULSAR);
            add(ClusterType.DATAPROXY);
            add(ClusterType.KAFKA);
            add(ClusterType.ELASTICSEARCH);
        }
    };

    /**
     * Check whether the cluster type is supported
     *
     * @param clusterType cluster type
     * @return cluster type
     */
    public static String checkType(String clusterType) {
        if (TYPE_SET.contains(clusterType)) {
            return clusterType;
        }
        throw new BusinessException(String.format("Unsupported cluster type=%s,"
                + " supported cluster types are: %s", clusterType, TYPE_SET));
    }
}
