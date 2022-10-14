package org.apache.inlong.agent.plugin.dbsync.metric;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 10:02
 * @Description:
 */

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.apache.inlong.agent.core.ha.JobHaDispatcher;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static final String METRIC_JOB_INFO_INF ="/metric/jobs/info";

    public static final String METRIC_ERROR_INF ="/metric/jobs/error";

    private ExecutorService executor = null;
    private DbSyncWebService metricService;
    private JobHaDispatcher jobHaDispatcher;

    public PrometheusMetricsServlet(DbSyncWebService metricService) {
        this.metricService = metricService;
    }

    @Override
    public void init() throws ServletException {
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("prometheus-stats"));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AsyncContext context = request.startAsync();
        executor.execute((() -> {
            HttpServletResponse res = (HttpServletResponse) context.getResponse();
            try {
                res.setStatus(HttpStatus.OK_200);
                res.setContentType("text/plain");
                PrometheusMetricsGenerator.generate(metricService, res.getOutputStream());
                context.complete();

            } catch (IOException e) {
                log.error("Failed to generate prometheus stats", e);
                res.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                context.complete();
            }
        }));
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static final Logger log = LogManager.getLogger(PrometheusMetricsServlet.class);
}

