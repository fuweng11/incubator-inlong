package org.apache.inlong.agent.core.ha.lb;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 功能描述：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 14:47
 * @Description:
 */
public class GenericDbSyncHostUsageImpl
        implements DbSyncHostUsage {
    // The interval for host usage check command
    private static final int CPU_CHECK_MILLIS = 1000;
    private double totalCpuLimit;
    private double cpuUsageSum = 0d;
    private int cpuUsageCount = 0;
    private OperatingSystemMXBean systemBean;
    private SystemResourceUsage usage;

    public GenericDbSyncHostUsageImpl(int hostUsageCheckIntervalMin,
            ScheduledExecutorService executorService) {
        this.systemBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.usage = new SystemResourceUsage();
        this.totalCpuLimit = getTotalCpuLimit();
        executorService.scheduleAtFixedRate(this::checkCpuLoad,
                0, CPU_CHECK_MILLIS, TimeUnit.MILLISECONDS);
        executorService.scheduleAtFixedRate(this::doCalculateBrokerHostUsage,
                0, hostUsageCheckIntervalMin, TimeUnit.MINUTES);
    }

    @Override
    public SystemResourceUsage getDbSyncHostUsage() {
        return usage;
    }

    private void checkCpuLoad() {
        cpuUsageSum += systemBean.getSystemCpuLoad();
        cpuUsageCount++;
    }

    @Override
    public void calculateDbSyncHostUsage() {
        checkCpuLoad();
        doCalculateBrokerHostUsage();
    }

    void doCalculateBrokerHostUsage() {
        SystemResourceUsage usage = new SystemResourceUsage();
        usage.setCpu(getCpuUsage());
        usage.setSystemMemory(getMemUsage());

        this.usage = usage;
    }

    private double getTotalCpuLimit() {
        return 100 * Runtime.getRuntime().availableProcessors();
    }

    private double getTotalCpuUsage() {
        double cpuUsage = cpuUsageSum / cpuUsageCount;
        cpuUsageSum = 0d;
        cpuUsageCount = 0;
        return cpuUsage;
    }

    private ResourceUsage getCpuUsage() {
        if (cpuUsageCount == 0) {
            return new ResourceUsage(0, totalCpuLimit);
        }
        return new ResourceUsage(getTotalCpuUsage() * totalCpuLimit, totalCpuLimit);
    }

    private ResourceUsage getMemUsage() {
        double total = ((double) systemBean.getTotalPhysicalMemorySize()) / (1024 * 1024);
        double free = ((double) systemBean.getFreePhysicalMemorySize()) / (1024 * 1024);
        return new ResourceUsage(total - free, total);
    }
}
