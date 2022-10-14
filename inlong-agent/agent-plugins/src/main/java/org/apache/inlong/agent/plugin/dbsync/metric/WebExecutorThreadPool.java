package org.apache.inlong.agent.plugin.dbsync.metric;


import java.util.concurrent.ThreadFactory;

import org.apache.inlong.agent.common.DefaultThreadFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/2 20:52
 * @Description:
 */
public class WebExecutorThreadPool extends ExecutorThreadPool {

    private final ThreadFactory threadFactory;

    public WebExecutorThreadPool(String namePrefix) {
        this(Runtime.getRuntime().availableProcessors(), namePrefix);
    }

    public WebExecutorThreadPool(int maxThreads, String namePrefix) {
        super(maxThreads);
        this.threadFactory = new DefaultThreadFactory(namePrefix) {

        };
    }

    @Override
    protected Thread newThread(Runnable job) {
        return threadFactory.newThread(job);
    }
}
