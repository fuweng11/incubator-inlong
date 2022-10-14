package org.apache.inlong.agent.mysql.connector.exception;

import java.io.IOException;

public class BinlogMissException extends IOException {
	
	private static final long serialVersionUID = -4081893322041088481L;
	
    public BinlogMissException(String errorCode){
        super(errorCode);
    }

    public BinlogMissException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public BinlogMissException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public BinlogMissException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public BinlogMissException(Throwable cause){
        super(cause);
    }

}
