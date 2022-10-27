/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.metrics.zhiyan;

import com.google.gson.Gson;
import com.tencent.teg.monitor.sdk.CurveReporter;
import com.tencent.teg.monitor.sdk.TegMonitor;

import org.apache.inlong.common.metric.MetricItemValue;
import org.apache.inlong.common.metric.MetricListener;
import org.apache.inlong.common.metric.MetricValue;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_CLUSTER_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_INLONG_STREAM_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_SINK_DATA_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_SINK_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_SOURCE_DATA_ID;
import static org.apache.inlong.dataproxy.metrics.DataProxyMetricItem.KEY_SOURCE_ID;

/**
 * ZhiyanMetricListener
 */
public class ZhiyanMetricListener implements MetricListener {

    public static final Logger LOG = LoggerFactory.getLogger(ZhiyanMetricListener.class);
    private static MetricValue ZERO = MetricValue.of(null, 0);

    private String appMark;
    private String metricGroup;
    private String env;
    private String instanceMark;

    /**
     * Constructor
     */
    public ZhiyanMetricListener() {
        try {
            TegMonitor.init();
            Map<String, String> envMap = System.getenv();
            this.appMark = envMap.getOrDefault("zhiyan.appMark", "app1");
            this.metricGroup = envMap.getOrDefault("zhiyan.metricGroup", "groupB");
            this.env = envMap.getOrDefault("zhiyan.env", "prod");
            this.instanceMark = envMap.getOrDefault("zhiyan.instanceMark", "127.0.0.1");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * snapshot
     * @param domain
     * @param itemValues
     */
    @Override
    public void snapshot(String domain, List<MetricItemValue> itemValues) {
        try {
            Gson gson = new Gson();
            for (MetricItemValue itemValue : itemValues) {
                CurveReporter reporter = TegMonitor.curveReporter()
                        .appMark(this.appMark)
                        .metricGroup(this.metricGroup)
                        .env(this.env)
                        .instanceMark(this.instanceMark);
                Map<String, String> dimensionMap = itemValue.getDimensions();
                // dimension
                reporter.tag(KEY_CLUSTER_ID, dimensionMap.getOrDefault(KEY_CLUSTER_ID, "-"));
                reporter.tag(KEY_SOURCE_ID, dimensionMap.getOrDefault(KEY_SOURCE_ID, "-"));
                reporter.tag(KEY_SOURCE_DATA_ID, dimensionMap.getOrDefault(KEY_SOURCE_DATA_ID, "-"));
                reporter.tag(KEY_INLONG_GROUP_ID, dimensionMap.getOrDefault(KEY_INLONG_GROUP_ID, "-"));
                reporter.tag(KEY_INLONG_STREAM_ID, dimensionMap.getOrDefault(KEY_INLONG_STREAM_ID, "-"));
                reporter.tag(KEY_SINK_ID, dimensionMap.getOrDefault(KEY_SINK_ID, "-"));
                reporter.tag(KEY_SINK_DATA_ID, dimensionMap.getOrDefault(KEY_SINK_DATA_ID, "-"));
                reporter.sumMetric("request_count", Math.random());
                // metric
                Map<String, MetricValue> metricMap = itemValue.getMetrics();
                reporter.sumMetric(DataProxyMetricItem.M_SEND_COUNT,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_COUNT, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SEND_SIZE,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_SIZE, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SEND_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_SUCCESS_COUNT, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SEND_SUCCESS_SIZE,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_SUCCESS_SIZE, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SEND_FAIL_COUNT,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SEND_FAIL_SIZE,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SEND_FAIL_SIZE, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_READ_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_READ_SUCCESS_COUNT, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_READ_SUCCESS_SIZE,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_READ_SUCCESS_SIZE, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_READ_FAIL_COUNT,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_READ_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_READ_FAIL_SIZE,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_READ_FAIL_SIZE, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_SINK_DURATION,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_SINK_DURATION, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_NODE_DURATION,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_NODE_DURATION, ZERO).value);
                reporter.sumMetric(DataProxyMetricItem.M_WHOLE_DURATION,
                        (double) metricMap.getOrDefault(DataProxyMetricItem.M_WHOLE_DURATION, ZERO).value);
                int result = reporter.report();
                LOG.info("zhiyan,dimensions:{},values:{},result:{}", dimensionMap, gson.toJson(metricMap), result);
            }
        } catch (Exception e) {
            LOG.error("reportZhiyan error:{}", e.getMessage(), e);
        }
    }

}
