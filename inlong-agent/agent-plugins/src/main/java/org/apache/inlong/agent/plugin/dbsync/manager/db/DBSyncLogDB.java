package org.apache.inlong.agent.plugin.dbsync.manager.db;

import java.nio.charset.Charset;
import java.util.HashMap;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;
import org.apache.inlong.agent.utils.JsonUtils.*;

public class DBSyncLogDB {
    private Environment myEnv;
    private Database db;
    private boolean closed;

    private static final String CONFIG_DB_NAME = "dbsync_log";
    private Charset charset;

    public DBSyncLogDB(Environment myEnv){
        this.myEnv = myEnv;
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        db = this.myEnv.openDatabase(null, CONFIG_DB_NAME, dbConfig);
        charset = Charset.forName(DBSyncConf.getInstance(null).getStringVar(ConfVars.CHARSET));
        closed = false;
    }

    public synchronized boolean putLog(String key, JSONObject obj) throws Exception{
        if (!closed) {
            byte[] theKey = key.getBytes(charset);
            byte[] theValue = obj.toString().getBytes(charset);
            OperationStatus status = db.put(null, new DatabaseEntry(theKey), new DatabaseEntry(theValue));
            if (status == OperationStatus.SUCCESS) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean delConfig(String key) throws Exception{
        if (!closed) {
            byte[] theKey = key.getBytes(charset);
            OperationStatus status = db.delete(null, new DatabaseEntry(theKey));
            if (status == OperationStatus.SUCCESS) {
                return true;
            }
        }
        return false;
    }

    public JSONObject getLog(String key){
        DatabaseEntry queryKey = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        queryKey.setData(key.getBytes(charset));

        OperationStatus status = db.get(null, queryKey, value, LockMode.DEFAULT);
        if (status == OperationStatus.SUCCESS) {
            return JSONObject.parseObject(new String(value.getData(), charset));
        }
        return null;
    }

    public HashMap<String, JSONObject> walkAllDb() throws Exception{

        Cursor cursor = null;

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
        
        try {
            cursor = db.openCursor(null, null);
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                String keyString = new String(foundKey.getData(), charset);
                String dataString = new String(foundData.getData(), charset);

                //
                JSONObject obj = JSONObject.parseObject(dataString);
                map.put(keyString, obj);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return map;
    }

    public void flush(){
        db.sync();
    }

    public synchronized void close(){
        if (!closed) {
            db.close();
            closed = true;
        }
    }

}
