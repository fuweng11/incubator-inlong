package org.apache.inlong.agent.plugin.dbsync.manager;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.inlong.agent.plugin.RegisterInfo;
import org.apache.inlong.agent.utils.JsonUtils.JSONArray;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public class InMemoryRegisterInfo implements RegisterInfo {

    private static final Logger logger = LogManager.getLogger(InMemoryRegisterInfo.class);

    private final List<OpResult> registerInfo = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void addInfo(Integer taskId, Integer status, Integer version, int result,
            String message) {
        lock.writeLock().lock();
        try {
            registerInfo.add(new OpResult(Long.parseLong(taskId), Integer.parseInt(status), lastOpTime,
                    version, result, message));
        } catch (Exception e) {
            logger.info("add register info error", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public JSONArray peekAllInfoJson() {
        lock.readLock().lock();
        try {
            JSONArray regInfoArray = new JSONArray();
            registerInfo.forEach(ops -> {
                regInfoArray.add(ops.toJson());
            });
            return regInfoArray;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String peekAllInfo() {
        return peekAllInfoJson().toJSONString();
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            registerInfo.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class OpResult {
        long taskId;
        Integer status;
        String lastOpTime;
        int version;
        int result;
        String message;

        public JSONObject toJson() {
            JSONObject opResult = new JSONObject();
            opResult.put(TASK_ID, taskId);
            opResult.put(STATUS, status);
            opResult.put(OP_TIME, lastOpTime);
            opResult.put(VERSION, version);
            opResult.put(RESULT, result);
            opResult.put(MESSAGE, message);
            return opResult;
        }
    }
}
