package org.apache.inlong.agent.plugin.dbsync.manager.db;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class DBSyncDbEnv {

    private Environment myEnv;
    private EntityStore store;

    // Our constructor does nothing
    public DBSyncDbEnv() {
    }

    // The setup() method opens the environment and store
    // for us.
    public void setup(File envHome, boolean readOnly,boolean bSync) throws DatabaseException {

        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        StoreConfig storeConfig = new StoreConfig();

        myEnvConfig.setReadOnly(readOnly);
        storeConfig.setReadOnly(readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and entity store if
        // they do not exist.
        myEnvConfig.setAllowCreate(!readOnly);
        storeConfig.setAllowCreate(!readOnly);

        myEnvConfig.setTransactional(bSync);
        storeConfig.setTransactional(bSync);

        myEnvConfig.setLockTimeout(10000, TimeUnit.MILLISECONDS);


        myEnvConfig.setTxnNoSyncVoid(false);
        myEnvConfig.setTxnWriteNoSyncVoid(false);

        // Open the environment and entity store
        myEnv = new Environment(envHome, myEnvConfig);
        store = new EntityStore(myEnv, "EntityStore", storeConfig);

    }

    // Return a handle to the entity store
    public EntityStore getEntityStore() {
        return store;
    }

    // Return a handle to the environment
    public Environment getEnv() {
        return myEnv;
    }

    // Close the store and environment
    public void close() {
        if (store != null) {
            try {
                store.sync();
                store.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing store: " + dbe.toString());
                //System.exit(-1);
            }
        }

        if (myEnv != null) {
            try {
                // Finally, close the store and environment.
                // myEnv.flushLog(true);
                myEnv.sync();
                myEnv.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " + dbe.toString());
                //System.exit(-1);
            }
        }
    }
}