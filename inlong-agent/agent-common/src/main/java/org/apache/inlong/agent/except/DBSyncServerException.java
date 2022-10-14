package org.apache.inlong.agent.except;

public class DBSyncServerException extends Exception {

    public DBSyncServerException() {
    }

    public DBSyncServerException(String message) {
        super(message);
    }

    public DBSyncServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBSyncServerException(Throwable cause) {
        super(cause);
    }

    public DBSyncServerException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static class JobInSwitchingState extends DBSyncServerException {
        public JobInSwitchingState(String message) {
            super(message);
        }
    }

    public static class JobInResetStatus extends DBSyncServerException {
        public JobInResetStatus(String message) {
            super(message);
        }
    }

    public static class JobResetFailed extends DBSyncServerException {
        public JobResetFailed(String message) {
            super(message);
        }
    }
}
