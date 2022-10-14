package org.apache.inlong.agent.core.job;

import com.google.common.collect.Sets;
import org.apache.inlong.agent.conf.JobProfile;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class JobConfManager {
    private final Set<JobProfile> delegate;

    public JobConfManager() {
        delegate = Sets.newConcurrentHashSet();
    }

    public synchronized void putConf(String instName, JobProfile conf) {
        delegate.add(conf);
    }

    /**
     * 是否正在采集解析
     * @param instName
     * @return true if parsing else false
     */
    public synchronized boolean isParsing(String instName) {
//        return inst2ConfMap.containsKey(instName);
        return delegate.stream().anyMatch(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()));
    }

    /**
     * 从当前正在采集的实例列表中 获取配置信息
     * @param instName
     * @return 存在则返回配置信息，不存在返回null
     */
    public synchronized JobProfile getParsingConfigByInstName(String instName) {
//        return inst2ConfMap.get(instName);
        return delegate.stream()
                .filter(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()))
                .findFirst()
                .orElse(null);
    }

    public synchronized boolean containsDatabase(String url) {
        return delegate.stream().anyMatch(conf -> conf.getDbSyncJobConf().containsDatabase(url));
    }

    public synchronized JobProfile getConfigByDatabase(String url, String serverId) {
        return delegate.stream()
                .filter(conf -> (conf.getDbSyncJobConf().containsDatabase(url) && (serverId.equals(
                        conf.getDbSyncJobConf().getServerId()))))
                .findFirst()
                .orElse(null);
    }

    public synchronized boolean containsTaskId(String taskId) {
//        return taskId2ConfMap.containsKey(taskId);
        return delegate.stream().anyMatch(conf -> conf.getDbSyncJobConf().getTaskIdList().contains(taskId));
    }

    public synchronized JobProfile getConfByTaskId(Integer taskId) {
//        return taskId2ConfMap.get(taskId);
        return delegate.stream()
                .filter(conf -> conf.getDbSyncJobConf().getTaskIdList().contains(taskId))
                .findFirst()
                .orElse(null);
    }

    public synchronized void removeConfByInst(String instName) {
//        JobProfile conf = inst2ConfMap.remove(instName);
//        if (conf != null) {
//            conf.getTaskIdList().forEach(taskId -> taskId2ConfMap.remove(taskId));
//        }
        delegate.removeIf(conf -> Objects.equals(instName, conf.getDbSyncJobConf().getJobName()));
    }

    public synchronized void removeConf(JobProfile conf) {
        delegate.remove(conf);
    }

    public synchronized int getConfSize() {
//        return inst2ConfMap.size();
        return delegate.size();
    }

    @Override
    public String toString() {
        return "JobConfManager{" +
                "delegate=" + Arrays.toString(delegate.toArray()) +
                '}';
    }
}
