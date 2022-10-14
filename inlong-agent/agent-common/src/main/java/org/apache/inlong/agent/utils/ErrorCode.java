package org.apache.inlong.agent.utils;

public class ErrorCode {

    /*
     * db exception
     */
    public static final String DB_EXCEPTION_RELAY_LOG_POSITION = "100000001";
    public static final String DB_EXCEPTION_TABLE_ID_NOT_FOUND = "100000002";
    public static final String DB_EXCEPTION_BINLOG_MISS = "100000003";
    public static final String DB_EXCEPTION_OTHER = "100000004";

    /*
     * op exception
     */
    public static final String OP_EXCEPTION_DUMPER_THREAD_EXIT = "200000001";
    public static final String OP_EXCEPTION_DUMPER_THREAD_EXIT_UNCAUGHT = "200000002";
    public static final String OP_EXCEPTION_DISPATCH_THREAD_EXIT_UNCAUGHT = "200000002";
}
