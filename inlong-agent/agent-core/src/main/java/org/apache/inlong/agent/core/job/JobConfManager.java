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

package org.apache.inlong.agent.core.job;

import com.google.common.collect.Sets;
import org.apache.inlong.agent.conf.JobProfile;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class JobConfManager {

    private final Set<JobProfile> delegate;

    public JobConfManager() {
        delegate = Sets.newConcurrentHashSet();
    }

    public synchronized void putConf(String instName, JobProfile conf) {
        delegate.add(conf);
    }

    public synchronized boolean isParsing(String instName) {
//        return inst2ConfMap.containsKey(instName);
        return delegate.stream().anyMatch(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()));
    }

    /**
     * get config info from current collecting instance list
     *
     * @param instName
     * @return if not exist, return null
     */
    public synchronized JobProfile getParsingConfigByInstName(String instName) {
        return delegate.stream()
                .filter(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()))
                .findFirst()
                .orElse(null);
    }

    public synchronized boolean containsDatabase(String url) {
        return delegate.stream().anyMatch(conf -> conf.getDbSyncJobConf().containsDatabase(url));
    }

    public synchronized JobProfile getConfigByDatabase(String url, String serverId) {
        return delegate.stream()
                .filter(conf -> (conf.getDbSyncJobConf().containsDatabase(url) && (serverId.equals(
                        conf.getDbSyncJobConf().getServerId()))))
                .findFirst()
                .orElse(null);
    }

    public synchronized boolean containsTaskId(String taskId) {
//        return taskId2ConfMap.containsKey(taskId);
        return delegate.stream().anyMatch(conf -> conf.getDbSyncJobConf().getTaskIdList().contains(taskId));
    }

    public synchronized JobProfile getConfByTaskId(Integer taskId) {
        return delegate.stream()
                .filter(conf -> conf.getDbSyncJobConf().getTaskIdList().contains(taskId))
                .findFirst()
                .orElse(null);
    }

    public synchronized void removeConfByInst(String instName) {

        delegate.removeIf(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()));
    }

    public synchronized void removeConf(JobProfile conf) {
        delegate.remove(conf);
    }

    public synchronized int getConfSize() {
        return delegate.size();
    }

    @Override
    public String toString() {
        return "JobConfManager{delegate=" + Arrays.toString(delegate.toArray()) + '}';
    }
}
