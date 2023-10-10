/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.loadmonitor;

import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.ConfigConstants;

import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LoadMonitor.class);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static LoadMonitor instance = null;
    private long intervalInMs = ConfigConstants.VAL_DEF_LOAD_COLLECT_INTERVALMS;
    private String netName = "eth1";
    private int accCnt = 0;
    private boolean fulled = false;
    private int maxCollSlotCnt = ConfigConstants.VAL_DEF_LOAD_MAX_ACC_PRINT;
    private static final AtomicInteger loadValue = new AtomicInteger(200);
    private int[] hisLoadList = new int[maxCollSlotCnt];
    private final ScheduledExecutorService executorService;

    public static LoadMonitor getInstance() {
        if (started.get() && instance != null) {
            return instance;
        }
        synchronized (LoadMonitor.class) {
            if (!started.get()) {
                instance = new LoadMonitor();
                instance.start();
            }
        }
        return instance;
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.executorService.shutdown();
        logger.info("[Load Monitor] stopped load monitor process");
    }

    public int getLoadValue() {
        return loadValue.get();
    }

    private LoadMonitor() {
        Map<String, String> comPropMap = CommonConfigHolder.getInstance().getProperties();
        if (comPropMap.containsKey(ConfigConstants.KEY_LOAD_NETWORK)) {
            this.netName = comPropMap.get(ConfigConstants.KEY_LOAD_NETWORK);
        }
        if (comPropMap.containsKey(ConfigConstants.KEY_LOAD_COLLECT_INTERVALMS)) {
            long tmpValue = Long.parseLong(comPropMap.get(ConfigConstants.KEY_LOAD_COLLECT_INTERVALMS));
            if (tmpValue >= ConfigConstants.VAL_MIN_LOAD_COLLECT_INTERVALMS
                    && tmpValue <= ConfigConstants.VAL_MAX_LOAD_COLLECT_INTERVALMS) {
                this.intervalInMs = tmpValue;
            } else {
                logger.warn("Illegal {} setting, {} must in [{}, {}]",
                        ConfigConstants.KEY_LOAD_COLLECT_INTERVALMS, tmpValue,
                        ConfigConstants.VAL_MIN_LOAD_COLLECT_INTERVALMS,
                        ConfigConstants.VAL_MAX_LOAD_COLLECT_INTERVALMS);
            }
        }
        if (comPropMap.containsKey(ConfigConstants.KEY_LOAD_MAX_ACC_PRINT)) {
            int tmpValue = Integer.parseInt(comPropMap.get(ConfigConstants.KEY_LOAD_MAX_ACC_PRINT));
            if (tmpValue >= ConfigConstants.VAL_MIN_LOAD_MAX_ACC_PRINT
                    && tmpValue <= ConfigConstants.VAL_MAX_LOAD_MAX_ACC_PRINT) {
                this.maxCollSlotCnt = tmpValue;
                this.hisLoadList = new int[this.maxCollSlotCnt];
            } else {
                logger.warn("Illegal {} setting, {} must in [{}, {}]",
                        ConfigConstants.KEY_LOAD_MAX_ACC_PRINT, tmpValue,
                        ConfigConstants.VAL_MIN_LOAD_MAX_ACC_PRINT,
                        ConfigConstants.VAL_MAX_LOAD_MAX_ACC_PRINT);
            }
        }
        this.executorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "LoadMonitor-Thread");
                        t.setPriority(Thread.NORM_PRIORITY);
                        return t;
                    }
                });
    }

    private void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        logger.info("java.library.path " + System.getProperty("java.library.path"));
        this.executorService.scheduleAtFixedRate(
                this, 0, intervalInMs, TimeUnit.MILLISECONDS);
        logger.info("[Load Monitor] started load monitor process");
    }

    @Override
    public void run() {
        try {
            // first, gather system info
            SysInfoItem probeStart = new SysInfoItem();
            readSysInfo(probeStart);
            // wait some time
            Thread.sleep(intervalInMs);
            // second, gather system info again
            SysInfoItem probeEnd = new SysInfoItem();
            readSysInfo(probeEnd);
            // third, calculate each indicator in schedule time
            // used cpu in this interval time
            double cpuUsed = (double) (probeEnd.cpuTotal - probeEnd.cpuIdle)
                    - (probeStart.cpuTotal - probeStart.cpuIdle);
            final double cpuPercent = cpuUsed / (probeEnd.cpuTotal - probeStart.cpuTotal) * 100;
            // memory
            double memUsed = probeEnd.memUsed / 1048576.0; // GB
            double memPercent = probeEnd.memUsed / 1.0 / probeEnd.memTotal * 100;
            // network
            final double netIn = (probeEnd.netIn - probeStart.netIn) / 1024.0
                    / (probeEnd.probTime - probeStart.probTime) * 8; // Mb/s
            final double netOut = (probeEnd.netOut - probeStart.netOut) / 1024.0
                    / (probeEnd.probTime - probeStart.probTime) * 8;
            // tcp connections
            final long tcpCon = probeEnd.tcpConn;
            // get load weight setting
            ConfigManager configManager = ConfigManager.getInstance();
            // calc load value
            if (cpuPercent < configManager.getCpuThresholdWeight()) {
                hisLoadList[accCnt] = (int) Math.ceil(cpuPercent * configManager.getCpuWeight()
                        + netIn * configManager.getNetInWeight()
                        + netOut * configManager.getNetOutWeight()
                        + tcpCon * configManager.getTcpWeight());
            } else {
                hisLoadList[accCnt] = 200;
            }
            long newLoad = 0L;
            for (int j : hisLoadList) {
                newLoad += j;
            }
            if (this.fulled) {
                newLoad /= hisLoadList.length;
            } else {
                newLoad /= (accCnt + 1);
            }
            int oldLoad = loadValue.getAndSet((int) newLoad);
            if (++accCnt >= maxCollSlotCnt) {
                this.accCnt = 0;
                this.fulled = true;
                logger.info("[Load Monitor] load calculate: curLoad={}, oldLoad={},"
                        + " weight is (cpu={}, net-in={}, net-out={}, tcp={}, cpuThreshold={}),"
                        + " value is (cpuPer={}, netIn={}, netOut={}, tcpCon={}, memPer={}),"
                        + ", maxSlots={}, collDur={}, hisVals={}",
                        oldLoad, loadValue.get(), configManager.getCpuWeight(), configManager.getNetInWeight(),
                        configManager.getNetOutWeight(), configManager.getTcpWeight(),
                        configManager.getCpuThresholdWeight(), cpuPercent, netIn, netOut, tcpCon, memPercent,
                        maxCollSlotCnt, intervalInMs, getSlotStrValues());
            }
        } catch (Throwable e) {
            logger.error("LoadCompute Exception, ", e);
        }
    }

    private String getSlotStrValues() {
        int cnt = 0;
        StringBuilder strBuff = new StringBuilder(512).append("[");
        for (int j : hisLoadList) {
            if (cnt++ > 0) {
                strBuff.append(",");
            }
            strBuff.append(j);
        }
        strBuff.append("]");
        return strBuff.toString();
    }

    private void readSysInfo(SysInfoItem probeValue) throws Exception {
        Sigar sigar = new Sigar();
        // read cup value
        for (Cpu cpu : sigar.getCpuList()) {
            probeValue.cpuIdle += cpu.getIdle();
            probeValue.cpuTotal += cpu.getTotal();
        }
        // read memory value
        Mem mem = sigar.getMem();
        probeValue.memUsed = mem.getUsed() / 1024;
        probeValue.memTotal = mem.getTotal() / 1024;
        // read network value
        for (String name : sigar.getNetInterfaceList()) {
            if (this.netName.equals(name)) {
                probeValue.probTime = System.currentTimeMillis() / 1000;
                NetInterfaceStat statStart = sigar.getNetInterfaceStat(name);
                probeValue.netIn = statStart.getRxBytes() / 1024;
                probeValue.netOut = statStart.getTxBytes() / 1024;
                break;
            }
        }
        // read tcp connections
        try {
            probeValue.tcpConn = sigar.getTcp().getCurrEstab();
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }
}
