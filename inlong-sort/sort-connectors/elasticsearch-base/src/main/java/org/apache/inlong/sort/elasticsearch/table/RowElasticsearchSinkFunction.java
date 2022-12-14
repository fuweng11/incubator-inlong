/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.elasticsearch.table;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.inlong.sort.base.dirty.DirtySinkHelper;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.metric.SinkMetricData;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

/** Sink function for converting upserts into Elasticsearch {@link ActionRequest}s. */
public class RowElasticsearchSinkFunction implements ElasticsearchSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RowElasticsearchSinkFunction.class);

    private final IndexGenerator indexGenerator;
    private final String docType;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final RequestFactory requestFactory;
    private final Function<RowData, String> createKey;
    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;
    private final String inlongMetric;
    private final String auditHostAndPorts;

    private final Function<RowData, String> createRouting;

    private transient RuntimeContext runtimeContext;

    private SinkMetricData sinkMetricData;
    private final DirtySinkHelper<Object> dirtySinkHelper;

    public RowElasticsearchSinkFunction(
            IndexGenerator indexGenerator,
            @Nullable String docType, // this is deprecated in es 7+
            SerializationSchema<RowData> serializationSchema,
            XContentType contentType,
            RequestFactory requestFactory,
            Function<RowData, String> createKey,
            @Nullable Function<RowData, String> createRouting,
            String inlongMetric,
            String auditHostAndPorts,
            DirtySinkHelper<Object> dirtySinkHelper) {
        this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
        this.docType = docType;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        this.contentType = Preconditions.checkNotNull(contentType);
        this.requestFactory = Preconditions.checkNotNull(requestFactory);
        this.createKey = Preconditions.checkNotNull(createKey);
        this.createRouting = createRouting;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.dirtySinkHelper = dirtySinkHelper;
    }

    @Override
    public void open(RuntimeContext ctx) {
        indexGenerator.open();
        this.runtimeContext = ctx;
        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withInlongAudit(auditHostAndPorts)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withRegisterMetric(RegisteredMetric.NORMAL)
                .build();
        if (metricOption != null) {
            sinkMetricData = new SinkMetricData(metricOption, runtimeContext.getMetricGroup());
        }
    }

    private void sendMetrics(byte[] document) {
        if (sinkMetricData != null) {
            sinkMetricData.invoke(1, document.length);
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext ctx) {
        this.runtimeContext = ctx;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (this.inlongMetric != null) {
            this.metricStateListState = context.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>(
                            INLONG_METRIC_STATE_NAME, TypeInformation.of(new TypeHint<MetricState>() {
                            })));
        }
        if (context.isRestored()) {
            metricState = MetricStateUtils.restoreMetricState(metricStateListState,
                    runtimeContext.getIndexOfThisSubtask(), runtimeContext.getNumberOfParallelSubtasks());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (sinkMetricData != null && metricStateListState != null) {
            MetricStateUtils.snapshotMetricStateForSinkMetricData(metricStateListState, sinkMetricData,
                    runtimeContext.getIndexOfThisSubtask());
        }
    }

    @Override
    public void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
        final byte[] document;
        try {
            document = serializationSchema.serialize(element);
        } catch (Exception e) {
            LOGGER.error(String.format("Serialize error, raw data: %s", element), e);
            dirtySinkHelper.invoke(element, DirtyType.SERIALIZE_ERROR, e);
            if (sinkMetricData != null) {
                sinkMetricData.invokeDirty(1, element.toString().getBytes(StandardCharsets.UTF_8).length);
            }
            return;
        }
        final String key;
        try {
            key = createKey.apply(element);
        } catch (Exception e) {
            LOGGER.error(String.format("Generate index id error, raw data: %s", element), e);
            dirtySinkHelper.invoke(element, DirtyType.INDEX_ID_GENERATE_ERROR, e);
            if (sinkMetricData != null) {
                sinkMetricData.invokeDirty(1, document.length);
            }
            return;
        }
        final String index;
        try {
            index = indexGenerator.generate(element);
        } catch (Exception e) {
            LOGGER.error(String.format("Generate index error, raw data: %s", element), e);
            dirtySinkHelper.invoke(element, DirtyType.INDEX_GENERATE_ERROR, e);
            if (sinkMetricData != null) {
                sinkMetricData.invokeDirty(1, document.length);
            }
            return;
        }
        addDocument(element, key, index, document, indexer);
    }

    private void addDocument(RowData element, String key, String index, byte[] document, RequestIndexer indexer) {
        DocWriteRequest<?> request;
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (key != null) {
                    request = requestFactory.createUpdateRequest(index, docType, key, contentType, document);
                    if (addRouting(request, element, document)) {
                        indexer.add((UpdateRequest) request);
                        sendMetrics(document);
                    }
                } else {
                    request = requestFactory.createIndexRequest(index, docType, key, contentType, document);
                    if (addRouting(request, element, document)) {
                        indexer.add((IndexRequest) request);
                        sendMetrics(document);
                    }
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                request = requestFactory.createDeleteRequest(index, docType, key);
                if (addRouting(request, element, document)) {
                    indexer.add((DeleteRequest) request);
                    sendMetrics(document);
                }
                break;
            default:
                LOGGER.error(String.format("The type of element should be 'RowData' only, raw data: %s", element));
                dirtySinkHelper.invoke(element, DirtyType.UNSUPPORTED_DATA_TYPE,
                        new RuntimeException("The type of element should be 'RowData' only."));
                if (sinkMetricData != null) {
                    sinkMetricData.invokeDirty(1, document.length);
                }
        }
    }

    private boolean addRouting(DocWriteRequest<?> request, RowData row, byte[] document) {
        if (null != createRouting) {
            try {
                String routing = createRouting.apply(row);
                request.routing(routing);
            } catch (Exception e) {
                LOGGER.error(String.format("Routing error, raw data: %s", row), e);
                dirtySinkHelper.invoke(row, DirtyType.INDEX_ROUTING_ERROR, e);
                if (sinkMetricData != null) {
                    sinkMetricData.invokeDirty(1, document.length);
                }
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowElasticsearchSinkFunction that = (RowElasticsearchSinkFunction) o;
        return Objects.equals(indexGenerator, that.indexGenerator)
                && Objects.equals(docType, that.docType)
                && Objects.equals(serializationSchema, that.serializationSchema)
                && contentType == that.contentType
                && Objects.equals(requestFactory, that.requestFactory)
                && Objects.equals(createKey, that.createKey)
                && Objects.equals(inlongMetric, that.inlongMetric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                indexGenerator,
                docType,
                serializationSchema,
                contentType,
                requestFactory,
                createKey,
                inlongMetric);
    }
}
