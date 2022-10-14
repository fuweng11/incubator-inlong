package org.apache.inlong.agent.mysql.connector.exception;

public class TableMapEventMissException extends CanalException{
	
	private static final long serialVersionUID = -4081893323041088481L;
	
    public TableMapEventMissException(String errorCode){
        super(errorCode);
    }

    public TableMapEventMissException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public TableMapEventMissException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public TableMapEventMissException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public TableMapEventMissException(Throwable cause){
        super(cause);
    }

}
