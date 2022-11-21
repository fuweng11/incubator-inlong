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

package org.apache.inlong.agent.metrics;

import com.google.gson.Gson;
import com.tencent.teg.monitor.sdk.CurveReporter;
import com.tencent.teg.monitor.sdk.TegMonitor;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.common.metric.MetricItemValue;
import org.apache.inlong.common.metric.MetricListener;
import org.apache.inlong.common.metric.MetricValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_COMPONENT_NAME;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_PLUGIN_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_STATISTICS_TYPE;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_JOB_FATAL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_JOB_RUNNING_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SINK_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SINK_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SOURCE_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SOURCE_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_FATAL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_RETRYING_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_RUNNING_COUNT;

public class AgentZhiyanMetricListener implements MetricListener {

    public static final Logger LOGGER = LoggerFactory.getLogger(AgentZhiyanMetricListener.class);
    private static MetricValue ZERO = MetricValue.of(null, 0);

    private String appMark;
    private String metricGroup;
    private String env;
    private String instanceMark;

    public static final String ZHIYAN_APPMARK = "zhiyan.appMark";
    public static final String DEFAULT_ZHIYAN_APPMARK = "app1";

    public static final String ZHIYAN_METRICGROUP = "zhiyan.metricGroup";
    public static final String DEFAULT_ZHIYAN_METRICGROUP = "groupB";

    public static final String ZHIYAN_ENV = "zhiyan.env";
    public static final String DEFAULT_ZHIYAN_ENV = "prod";

    public static final String ZHIYAN_INSTANCEMARK = "zhiyan.instanceMark";
    public static final String DEFAULT_ZHIYAN_INSTANCEMARK = "127.0.0.1";

    /**
     * Constructor
     */
    public AgentZhiyanMetricListener() {
        try {
            TegMonitor.init();
            this.appMark = AgentConfiguration.getAgentConf().get(ZHIYAN_APPMARK, DEFAULT_ZHIYAN_APPMARK);
            this.metricGroup = AgentConfiguration.getAgentConf().get(ZHIYAN_METRICGROUP, DEFAULT_ZHIYAN_METRICGROUP);
            this.env = AgentConfiguration.getAgentConf().get(ZHIYAN_ENV, DEFAULT_ZHIYAN_ENV);
            this.instanceMark = AgentConfiguration.getAgentConf().get(ZHIYAN_INSTANCEMARK, DEFAULT_ZHIYAN_INSTANCEMARK);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

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
                String pluginId = dimensionMap.getOrDefault(KEY_PLUGIN_ID, "-");
                if (!StringUtils.equals(pluginId, "MemoryChannel")) {
                    reporter.tag(KEY_STATISTICS_TYPE, "entire");
                } else {
                    reporter.tag(KEY_STATISTICS_TYPE, "plugins");
                }
                reporter.tag(KEY_PLUGIN_ID, pluginId);
                reporter.tag(KEY_INLONG_GROUP_ID, dimensionMap.getOrDefault(KEY_INLONG_GROUP_ID, "-"));
                reporter.tag(KEY_INLONG_STREAM_ID, dimensionMap.getOrDefault(KEY_INLONG_STREAM_ID, "-"));
                reporter.tag(KEY_COMPONENT_NAME, dimensionMap.getOrDefault(KEY_COMPONENT_NAME, "-"));
                reporter.sumMetric("request_count", Math.random());

                // metric
                Map<String, MetricValue> metricMap = itemValue.getMetrics();
                reporter.sumMetric(M_JOB_RUNNING_COUNT,
                        (double) metricMap.getOrDefault(M_JOB_RUNNING_COUNT, ZERO).value);
                reporter.sumMetric(M_JOB_FATAL_COUNT,
                        (double) metricMap.getOrDefault(M_JOB_FATAL_COUNT, ZERO).value);
                reporter.sumMetric(M_TASK_RUNNING_COUNT,
                        (double) metricMap.getOrDefault(M_TASK_RUNNING_COUNT, ZERO).value);
                reporter.sumMetric(M_TASK_RETRYING_COUNT,
                        (double) metricMap.getOrDefault(M_TASK_RETRYING_COUNT, ZERO).value);
                reporter.sumMetric(M_TASK_FATAL_COUNT,
                        (double) metricMap.getOrDefault(M_TASK_FATAL_COUNT, ZERO).value);
                reporter.sumMetric(M_SINK_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(M_SINK_SUCCESS_COUNT, ZERO).value);
                reporter.sumMetric(M_SINK_FAIL_COUNT,
                        (double) metricMap.getOrDefault(M_SINK_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(M_SOURCE_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(M_SOURCE_SUCCESS_COUNT, ZERO).value);
                reporter.sumMetric(M_SOURCE_FAIL_COUNT,
                        (double) metricMap.getOrDefault(M_SOURCE_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_READ_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_READ_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_SEND_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_SEND_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_READ_FAIL_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_READ_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_SEND_FAIL_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_SEND_FAIL_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_READ_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_READ_SUCCESS_COUNT, ZERO).value);
                reporter.sumMetric(M_PLUGIN_SEND_SUCCESS_COUNT,
                        (double) metricMap.getOrDefault(M_PLUGIN_SEND_SUCCESS_COUNT, ZERO).value);
                int result = reporter.report();
                LOGGER.debug("agent-zhiyan,dimensions:{},values:{},result:{}", dimensionMap, gson.toJson(metricMap),
                        result);
            }
        } catch (Exception e) {
            LOGGER.error("agent report Zhiyan error:{}", e.getMessage(), e);
        }

    }

}
