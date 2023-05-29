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

package org.apache.inlong.sort.cdc.base.source.meta.events;

import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link IncrementalSourceEnumerator} sends to {@link
 * IncrementalSourceReader} to notify the finished snapshot splits has been received, i.e.
 * acknowledge for {@link FinishedSnapshotSplitsReportEvent}.
 * Copy from com.ververica:flink-cdc-base:2.3.0.
 */
public class FinishedSnapshotSplitsAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<String> finishedSplits;

    public FinishedSnapshotSplitsAckEvent(List<String> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }

    public List<String> getFinishedSplits() {
        return finishedSplits;
    }

    @Override
    public String toString() {
        return "FinishedSnapshotSplitsAckEvent{" + "finishedSplits=" + finishedSplits + '}';
    }
}
