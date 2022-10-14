package org.apache.inlong.agent.mysql.relayLog.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class RelayLogPosErrorException extends NestableRuntimeException {

    private static final long serialVersionUID = 7206217564574091404L;

    public RelayLogPosErrorException(String errorCode){
        super(errorCode);
    }

    public RelayLogPosErrorException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public RelayLogPosErrorException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public RelayLogPosErrorException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public RelayLogPosErrorException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
