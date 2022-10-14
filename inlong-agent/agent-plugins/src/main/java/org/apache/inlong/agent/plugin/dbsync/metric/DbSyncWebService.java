package org.apache.inlong.agent.plugin.dbsync.metric;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;
import org.apache.inlong.agent.core.ha.JobHaDispatcher;
import org.apache.inlong.agent.core.ha.JobHaInfo;
import org.apache.inlong.agent.conf.DBSyncConf;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.servlet.ServletHolder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/2 20:47
 * @Description:
 */
public class DbSyncWebService {

    private Logger logger = LogManager.getLogger(DbSyncWebService.class);

    private WebService webService;

    private DBSyncConf conf;

    private JobHaDispatcher jobHaDispatcher;

    public DbSyncWebService(DBSyncConf conf, JobHaDispatcher jobHaDispatcher) {
        this.conf = conf;
        this.jobHaDispatcher = jobHaDispatcher;
    }
    public void start() {
        try {
            this.webService = new WebService(conf);
            Map<String, Object> attributeMap = Maps.newHashMap();
            attributeMap.put("dbSync", this);
            JmxServlet jmxServlet = new JmxServlet(this);
            PrometheusMetricsServlet prometheusMetricsServlet = new PrometheusMetricsServlet(this);
            this.webService.addServlet(JmxServlet.JMX_JOB_INFO_INF,
                    new ServletHolder(jmxServlet),
                    false, attributeMap);
            this.webService.addServlet(JmxServlet.JMX_JOB_RUNNING_INFO_INF,
                    new ServletHolder(jmxServlet),
                    false, attributeMap);
            this.webService.addServlet(JmxServlet.JMX_JOB_WARN_INF,
                    new ServletHolder(jmxServlet),
                    false, attributeMap);
            this.webService.addServlet("/metric",
                    new ServletHolder(prometheusMetricsServlet),
                    false, attributeMap);
            webService.start();
        } catch (Exception e) {
            logger.error("start MetricService has error e = {}", e);
        }
    }

    public String getHaJobInfo() {
        return JSONObject.toJSONString(jobHaDispatcher.getHaJobInfList(),
                SerializerFeature.PrettyFormat);
    }

    public String getRunningJobInfo() {
        Set<JobHaInfo> jobSet = jobHaDispatcher.getHaJobInfList();
        return JSONObject.toJSONString(jobSet,
                SerializerFeature.PrettyFormat);
    }

    public String getWarnJobInfo() {
        Set<JobHaInfo> jobSet = jobHaDispatcher.getHaJobInfList();
        Set<JobHaInfo> warinJobInfo = null;
        if (jobSet != null && jobSet.size() > 0) {
            for (JobHaInfo info : jobSet) {
                if (StringUtils.isEmpty(info.getSyncPosition())) {
                    if (warinJobInfo == null) {
                        warinJobInfo = new HashSet<>();
                    }
                    warinJobInfo.add(info);
                }
            }
        }
        return JSONObject.toJSONString(warinJobInfo,
                SerializerFeature.PrettyFormat);
    }

    public void stop() {
        try {
            webService.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
