package org.apache.inlong.agent.plugin.dbsync.metric;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/2 21:06
 * @Description:
 */
public class DbsyncServerConnector extends ServerConnector {

    // Throttle down the accept rate to limit the number of active TCP connections
    private final Semaphore semaphore = new Semaphore(10000);

    /**
     * @param server
     * @param acceptors
     * @param selectors
     */
    public DbsyncServerConnector(Server server, int acceptors, int selectors) {
        super(server, acceptors, selectors);
    }

    /**
     * @param server
     * @param acceptors
     * @param selectors
     * @param sslContextFactory
     */
    public DbsyncServerConnector(Server server, int acceptors, int selectors, SslContextFactory sslContextFactory) {
        super(server, acceptors, selectors, sslContextFactory);
    }

    @Override
    public void accept(int acceptorID) throws IOException {
        try {
            semaphore.acquire();
            super.accept(acceptorID);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void onEndPointClosed(EndPoint endp) {
        semaphore.release();
        super.onEndPointClosed(endp);
    }
}

