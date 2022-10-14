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

public class JmxServlet
        extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = LogManager.getLogger(JmxServlet.class);

    public static final String JMX_JOB_INFO_INF ="/jmx/jobs/info";

    public static final String JMX_JOB_RUNNING_INFO_INF ="/jmx/jobs/running/info";

    public static final String JMX_JOB_WARN_INF ="/jmx/jobs/warn/info";

    private ExecutorService executor = null;
    private DbSyncWebService metricService;
    private JobHaDispatcher jobHaDispatcher;

    public JmxServlet(DbSyncWebService metricService) {
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
                byte[] data = null;
                if (JMX_JOB_INFO_INF.equals(request.getContextPath())) {
                    data = metricService.getHaJobInfo().getBytes();
                } else if (JMX_JOB_RUNNING_INFO_INF.equals(request.getContextPath())) {
                    data = metricService.getRunningJobInfo().getBytes();
                } else if (JMX_JOB_WARN_INF.equals(request.getContextPath())) {
                    data = metricService.getWarnJobInfo().getBytes();
                } else {
                    data = "hello DbSync!".getBytes();
                }
                try {
                    res.getOutputStream().write(data, 0, data.length);
                } catch (Exception e){
                    logger.error("[{}] generate has exception = {}", request.getContextPath(), e);
                }
                context.complete();

            } catch (Exception e) {
                log.error("Failed to generate DbSync stats", e);
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

    private static final Logger log = LogManager.getLogger(JmxServlet.class);
}

