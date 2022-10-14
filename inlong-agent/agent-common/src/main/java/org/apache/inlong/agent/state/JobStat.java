package org.apache.inlong.agent.state;


public class JobStat {
    public enum State{
        RUN,
        STOP,
        PAUSE,
        INIT,
        ERROR
    }

    public enum TaskStat{
        NORMAL,
        SWITCHING,
        SWITCHED,
        RESETTING,
        RESET,
        PAUSE,
        DELETE;

    }
}
