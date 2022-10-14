package org.apache.inlong.agent.plugin.dbsync.util;

/**
 * Created by lamberliu on 2017/10/24.
 */
public class DBSyncErrMsg {
    public static final String DBSYNC_SUCCESS = "SUCCESS";

    public static final String SYS_CONFIG_ERROR = "ERROR-1-TDBANK-DBSync|20001|ERROR|ERROR_CONFIG|";

    public static final String IO_ERROR = "ERROR-1-TDBANK-DBSync|20002|ERROR|ERROR_OPERATE_FILE|";

    public static final String RUNTIME_ERROR = "ERROR-1-TDBANK-DBSync|20003|ERROR|ERROR_RUNTIME_EXCEPTION|";

    public static final String FIND_BINGLOG_ERROR = "ERROR-1-TDBANK-DBSync|20004|ERROR|ERROR_FIND_BINLOG_POSITION|";

    public static final String DATABASE_INFO_ERROR = "ERROR-0-TDBANK-DBSync|10002|WARN|ERROR_DATABASE_INFO|";

    public static final String PARSE_BINLOG_ERROR = "ERROR-1-TDBANK-DBSync|20005|ERROR|ERROR_PARSE_BINLOG|";

    public static final String GET_POSITION_ERROR = "ERROR-1-TDBANK-DBSync|20006|ERROR|ERROR_GET_POSITION|";

    public static final String DECODE_BINLOG_ERROR = "ERROR-1-TDBANK-DBSync|20007|ERROR|ERROR_DECODE_BINLOG|";

    public static final String DISPATCH_EVENT_ERROR = "ERROR-1-TDBANK-DBSync|20008|ERROR|ERROR_DISPATCH_EVENT|";

    public static final String DUMP_ERROR = "ERROR-1-TDBANK-DBSync|20009|ERROR|ERROR_DUMP_BINLOG|";

    public static final String USER_CONFIG_ERROR = "ERROR-0-TDBANK-DBSync|10004|ERROR|ERROR_CONFIG|";

    public static final String TASK_OP_ERROR = "ERROR-1-TDBANK-DBSync|20010|ERROR|ERROR_OPERATE_TASK|";

    public static final String SWITCH_DB_ERROR = "ERROR-1-TDBANK-DBSync|20011|ERROR|ERROR_SWITCH_DATABASE|";

    public static final String BDB_ERROR = "ERROR-1-TDBANK-DBSync|20012|ERROR|ERROR_OPERATE_BDB|";

    public static final String CONNECT_ERROR = "ERROR-1-TDBANK-DBSync|20013|ERROR|ERROR_CONNECT_TO_SERVER|";

    public static final String TDM_THREAD_ERROR = "ERROR-1-TDBANK-DBSync|20014|ERROR|ERROR_TDM_THREAD|";

    public static final String JOBMANAGER_THREAD_ERROR = "ERROR-1-TDBANK-DBSync|20015|ERROR|ERROR_JOBMANAGER_THREAD|";

    public static final String JOBMONITOR_THREAD_ERROR = "ERROR-1-TDBANK-DBSync|20016|ERROR|ERROR_JOBMONITOR_THREAD|";


}
