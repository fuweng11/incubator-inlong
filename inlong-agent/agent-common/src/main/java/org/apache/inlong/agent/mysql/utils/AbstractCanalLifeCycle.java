package org.apache.inlong.agent.mysql.utils;

import org.apache.inlong.agent.mysql.connector.exception.CanalException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 基本实现
 * 
 * @author jianghang 2012-7-12 上午10:11:07
 * @version 1.0.0
 */
public abstract class AbstractCanalLifeCycle implements CanalLifeCycle {

    protected volatile boolean running = false; // 是否处于运行中
    public static final Logger logger              = LogManager.getLogger(AbstractCanalLifeCycle.class);

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new CanalException(this.getClass().getName() + " has startup , don't repeat start");
        }

        running = true;
    }

    public void stop() {
        logger.info("stop parse");
        if (!running) {
            throw new CanalException(this.getClass().getName() + " isn't start , please check");
        }

        running = false;
    }

}
