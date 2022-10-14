package org.apache.inlong.agent.except;

public class DataSourceConfigException extends Exception {
    public DataSourceConfigException(String msg) {
        super(msg);
    }

    public DataSourceConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSourceConfigException(Throwable cause) {
        super(cause);
    }

    public DataSourceConfigException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DataSourceConfigException() {
    }

    public static class JobNotFoundException extends DataSourceConfigException {
        public JobNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class InvalidCharsetNameException extends DataSourceConfigException {
        public InvalidCharsetNameException(String msg) {
            super(msg);
        }
    }

    public static class JobSizeExceedMaxException extends DataSourceConfigException {
        public JobSizeExceedMaxException(String msg) {
            super(msg);
        }
    }
}
