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

package org.apache.inlong.agent.core.dbsync;

import org.apache.inlong.agent.metrics.MetricReport;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class DbAgentDumpMetric implements MetricReport {

    private AtomicLong dumpSize = new AtomicLong(0);

    private volatile String binlogFile;

    private volatile long position;

    private volatile int lastEventType;

    private volatile long when;

    public long addDumpSize(long dumpSize) {
        return this.dumpSize.addAndGet(dumpSize);
    }

    public String getAndResetMetricInfo() {
        String dumpSizeStr = getFileSize(getAndResetDumpSize());
        return "dumpSize:" + dumpSizeStr + ",lastEventType:" + lastEventType + ",binlogFile:"
                + binlogFile + ",position:" + position + ",timestamp:" + when;
    }

    private long getAndResetDumpSize() {
        return dumpSize.getAndSet(0L);
    }

    public String getFileSize(long size) {
        double length = size;

        if (length < 1024) {
            return length + "B";
        } else {
            length = length / 1024.0;
        }

        if (length < 1024) {
            return Math.round(length * 100) / 100.0 + "KB";
        } else {
            length = length / 1024.0;
        }
        if (length < 1024) {
            return Math.round(length * 100) / 100.0 + "MB";
        } else {
            return Math.round(length / 1024 * 100) / 100.0 + "GB";
        }
    }

    @Override
    public String report() {
        return null;
    }
}
