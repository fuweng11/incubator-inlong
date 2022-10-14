package org.apache.inlong.agent.plugin.dbsync.metric;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 10:05
 * @Description:
 */

import io.prometheus.client.Collector;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class PrometheusMetricsGenerator {

    private static Logger logger = LogManager.getLogger(PrometheusMetricsGenerator.class);

    static {
        DefaultExports.initialize();
    }

    public static void generate(DbSyncWebService metricService, OutputStream out) throws
            IOException {
        try {
            byte[] data = metricService.getHaJobInfo().getBytes();
            out.write(data, 0, data.length);
        } catch (Exception e){
            logger.error("generate has exception = {}", e);
        }
    }

    static String getTypeStr(Collector.Type type) {
        switch (type) {
            case COUNTER:
                return "counter";
            case GAUGE:
                return "gauge";
            case SUMMARY        :
                return "summary";
            case HISTOGRAM:
                return "histogram";
            default:
                return "untyped";
        }
    }

}
