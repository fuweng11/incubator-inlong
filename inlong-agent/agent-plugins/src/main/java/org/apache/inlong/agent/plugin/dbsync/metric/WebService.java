package org.apache.inlong.agent.plugin.dbsync.metric;


import com.google.common.collect.Lists;
import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Web Service embedded into Pulsar
 */
public class WebService implements AutoCloseable {


    private static final Logger log = LogManager.getLogger(WebService.class);


    private static final String MATCH_ALL = "/*";

    public static final String HANDLER_CACHE_CONTROL = "max-age=3600";
    public static final int MAX_CONCURRENT_REQUESTS = 1024; // make it configurable?

    private final Server server;
    private final List<Handler> handlers;
    private final WebExecutorThreadPool webServiceExecutor;

    private final ServerConnector httpConnector;
    private JettyStatisticsCollector jettyStatisticsCollector;

    public WebService(DBSyncConf conf) {
        this.handlers = Lists.newArrayList();
        this.webServiceExecutor = new WebExecutorThreadPool(
                DBSyncConf.getInstance(null).getIntVar(ConfVars.LOCAL_IO_NUM),
                "dbsync-web");
        this.server = new Server(webServiceExecutor);
        List<ServerConnector> connectors = new ArrayList<>();

        Integer port = DBSyncConf.getInstance(null).getIntVar(ConfVars.LOCAL_WEB_PORT);
        if (port != null) {
            httpConnector = new DbsyncServerConnector(server, 1, 1);
            httpConnector.setPort(port);
            httpConnector.setHost("127.0.0.1");
            connectors.add(httpConnector);
        } else {
            httpConnector = null;
        }
        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(WebService.MAX_CONCURRENT_REQUESTS / connectors.size()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication, Map<String,Object> attributeMap) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(path);
        context.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach((key, value) -> {
                context.setAttribute(key, value);
            });
        }
//        context.addFilter(new FilterHolder(
//                new MaxRequestSizeFilter(
//                        DBSyncConf.getInstance(null).getLongVar(ConfVars.MAX_WEB_REQUEST_SIZE))),
//                MATCH_ALL, EnumSet.allOf(DispatcherType.class));
        handlers.add(context);
    }

    public void start() throws Exception{
        try {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            Slf4jRequestLog requestLog = new Slf4jRequestLog();
            requestLog.setExtended(true);
            requestLog.setLogTimeZone(TimeZone.getDefault().getID());
            requestLog.setLogLatency(true);
            requestLogHandler.setRequestLog(requestLog);
            handlers.add(0, new ContextHandlerCollection());
            handlers.add(requestLogHandler);

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

            HandlerCollection handlerCollection = new HandlerCollection();
            handlerCollection.setHandlers(new Handler[] { contexts, new DefaultHandler(), requestLogHandler });

            // Metrics handler
            StatisticsHandler stats = new StatisticsHandler();
            stats.setHandler(handlerCollection);
            try {
                jettyStatisticsCollector = new JettyStatisticsCollector(stats);
                jettyStatisticsCollector.register();
            } catch (IllegalArgumentException e) {
                // Already registered. Eg: in unit tests
            }
            handlers.add(stats);

            server.setHandler(stats);
            server.start();

            if (httpConnector != null) {
                log.info("HTTP Service started at http://{}:{}", httpConnector.getHost(), httpConnector.getLocalPort());
            } else {
                log.info("HTTP Service disabled");
            }

        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void close() throws Exception{
        try {
            server.stop();
            if (jettyStatisticsCollector != null) {
                try {
                    CollectorRegistry.defaultRegistry.unregister(jettyStatisticsCollector);
                } catch (Exception e) {
                    // ignore any exception happening in unregister
                    // exception will be thrown for 2. instance of WebService in tests since
                    // the register supports a single JettyStatisticsCollector
                }
                jettyStatisticsCollector = null;
            }
            webServiceExecutor.join();
            log.info("Web service closed");
        } catch (Exception e) {
            throw e;
        }
    }

}
