package org.apache.inlong.agent.plugin.dbsync.manager.db;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import org.apache.inlong.agent.utils.JsonUtils.*;

public class DBSyncConfigDB {
    private Environment myEnv;
    private Database db;
    private boolean closed;

    private static final String CONFIG_DB_NAME = "dbsync_config";

    public DBSyncConfigDB(Environment myEnv){
        this.myEnv = myEnv;
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        db = this.myEnv.openDatabase(null, CONFIG_DB_NAME, dbConfig);
        closed = false;
    }

    public synchronized boolean putConfig(String key, JSONObject obj) throws Exception{
        if (!closed) {
            byte[] theKey = key.getBytes(StandardCharsets.UTF_8);
            byte[] theValue = obj.toString().getBytes(StandardCharsets.UTF_8);
            OperationStatus status = db.put(null, new DatabaseEntry(theKey), new DatabaseEntry(theValue));
            if (status == OperationStatus.SUCCESS) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean delConfig(String key) throws Exception{
        if (!closed) {
            byte[] theKey = key.getBytes(StandardCharsets.UTF_8);
            OperationStatus status = db.delete(null, new DatabaseEntry(theKey));
            if (status == OperationStatus.SUCCESS) {
                return true;
            }
        }
        return false;
    }

    public HashMap<String, JSONObject> walkAllDb() throws Exception{

        Cursor cursor = null;

        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
        
        try {
            cursor = db.openCursor(null, null);
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                String keyString = new String(foundKey.getData(), StandardCharsets.UTF_8);
                String dataString = new String(foundData.getData(), StandardCharsets.UTF_8);

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
