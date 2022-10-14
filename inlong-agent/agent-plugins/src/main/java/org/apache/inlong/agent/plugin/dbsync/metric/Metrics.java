package org.apache.inlong.agent.plugin.dbsync.metric;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 10:09
 * @Description:
 */
public class Metrics {

    final Map<String, Object> metrics;

    @JsonInclude(content = Include.NON_EMPTY)
    final Map<String, String> dimensions;

    public Metrics() {
        metrics = Maps.newTreeMap();
        dimensions = Maps.newHashMap();
    }

    // hide constructor
    protected Metrics(Map<String, String> unmodifiableDimensionMap) {
        this.metrics = Maps.newTreeMap();
        this.dimensions = unmodifiableDimensionMap;
    }

    /**
     * Creates a metrics object with the dimensions map immutable.
     *
     * @param dimensionMap
     * @return
     */
    public static Metrics create(Map<String, String> dimensionMap) {
        // make the dimensions map unmodifiable and immutable;
        Map<String, String> map = Maps.newTreeMap();
        map.putAll(dimensionMap);
        return new Metrics(Collections.unmodifiableMap(map));
    }

    public void put(String metricsName, Object value) {
        metrics.put(metricsName, value);
    }

    public Map<String, Object> getMetrics() {
        return Collections.unmodifiableMap(this.metrics);
    }

    public void putAll(Map<String, Object> metrics) {
        this.metrics.putAll(metrics);
    }

    public Map<String, String> getDimensions() {
        // already unmodifiable
        return this.dimensions;
    }

    public String getDimension(String dimensionName) {
        return dimensions.get(dimensionName);
    }

    @Override
    public int hashCode() {
        // the business key will be my metrics dimension [ immutable ]
        return Objects.hashCode(dimensions);
    }

    @Override
    public boolean equals(Object obj) {
        // the business key will be my metrics dimension [ immutable ]
        return (obj instanceof Metrics) && Objects.equal(this.dimensions, ((Metrics) obj).dimensions);
    }

    @Override
    public String toString() {
        return String.format("dimensions=[%s], metrics=[%s]", dimensions, metrics);
    }
}

