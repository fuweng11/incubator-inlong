package org.apache.inlong.agent.core.monitor;

import org.apache.commons.lang.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.DBSyncConf;
import org.apache.inlong.agent.conf.DBSyncConf.ConfVars;
import org.apache.inlong.agent.conf.DBSyncJobConf;
import org.apache.inlong.agent.conf.MysqlTableConf;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.ha.JobHaDispatcher;
import org.apache.inlong.agent.core.job.DBSyncJob;
import org.apache.inlong.agent.core.job.JobConfManager;
import org.apache.inlong.agent.core.job.JobManager;
import org.apache.inlong.agent.entites.ProxyEvent;
import org.apache.inlong.agent.entites.JobMetricInfo;
import org.apache.inlong.agent.mysql.protocol.position.LogPosition;
import org.apache.inlong.agent.state.JobStat;
import org.apache.inlong.agent.utils.HandleFailedMessage;
import org.apache.inlong.agent.utils.HandleFailedMessage.BusMessageQueueCallback;
import org.apache.inlong.agent.utils.JsonUtils.JSONArray;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.inlong.agent.utils.SnowFlake;
import org.apache.inlong.agent.utils.TDManagerConn;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.network.HttpProxySender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.BUS_DELIMITER;
import static org.apache.inlong.agent.constant.AgentConstants.MSG_INDEX_KEY;

public class JobMonitor extends AbstractDaemon {

    protected static final Logger LOGGER = LoggerFactory.getLogger(JobMonitor.class);
    private static HttpProxySender proxySender;
    private final int resettingCheckInterval;
    private final int updateZkInterval;
    private ConcurrentHashMap<String, DBSyncJob> monitorJobs;
    private ConcurrentHashMap<String, Long> jobHb;
    private ConcurrentHashMap<String, LogPosition> jobSenderPosition;
    private ConcurrentHashMap<String, LogPosition> jobNewestPosition;
    private DBSyncConf config;
    private volatile boolean running = false;
    private ConcurrentHashMap<String, Long> sendedAlartMap;
    /**
     * key: serverId, value: snowflake
     */
    private ConcurrentHashMap<Long, SnowFlake> idGeneratorMap;
    //statistic info
    private ConcurrentHashMap<String, AtomicLong> statisticData;
    private ConcurrentHashMap<String, AtomicLong> statisticDataPackage;
    private HandleFailedMessage handleFailedMessage;
    private LinkedBlockingQueue<StatisticInfo> sInfos;
    private Random random = new Random();
    private LinkedBlockingQueue<JobAlarmMsg> alarmMsg;
    private LinkedBlockingQueue<JSONObject> holdHeartbeats;
    private boolean bStoped = false;
    private boolean monitorFlag = true;
    private long jobBlockTime = 0L;
    private String metricBid;
    private String metricTid;
    private long connInterval;
    private JobManager jobManager;
    private JobConfManager jobConfManager;//TODO:初始化
    private JobHaDispatcher jobHaDispatcher;//TODO:初始化

    //TODO：线程异常捕获处理setUncaughtExceptionHandler
    public JobMonitor(AgentManager agentManager) {
        this.jobManager = agentManager.getJobManager();
        monitorJobs = new ConcurrentHashMap<>();
        jobHb = new ConcurrentHashMap<>();
        statisticData = new ConcurrentHashMap<>();
        handleFailedMessage = HandleFailedMessage.getInstance();
        statisticDataPackage = new ConcurrentHashMap<>();
        idGeneratorMap = new ConcurrentHashMap<>();
        sInfos = new LinkedBlockingQueue<>();
        sendedAlartMap = new ConcurrentHashMap<>();
        try {
            ProxyClientConfig proxyClientConfig = new ProxyClientConfig(config.getLocalIp(), true,
                    config.getStringVar(ConfVars.TDMANAGER_ADDRESS_OLD),
                    config.getIntVar(ConfVars.TDMANAGER_PORT_OLD),
                    config.getStringVar(ConfVars.METRICS_BID),
                    config.getStringVar(ConfVars.TDMANAGER_BUS_NETTAG), "", "");
            proxyClientConfig.setCleanHttpCacheWhenClosing(true);
            proxyClientConfig.setDiscardOldMessage(config.getBooleanVar(ConfVars.METRICS_DISCARD_OLD_MESSAGE));
            proxySender = new HttpProxySender(proxyClientConfig);
            metricBid = config.getStringVar(ConfVars.METRICS_BID);
            metricTid = config.getStringVar(ConfVars.METRICS_TID);
            handleFailedMessage.setSender(proxySender);
        } catch (Exception e) {
            LOGGER.error("init bus sender fail", e);
        }

        alarmMsg = new LinkedBlockingQueue<>();
        holdHeartbeats = new LinkedBlockingQueue<>();
        jobSenderPosition = new ConcurrentHashMap<>();
        jobNewestPosition = new ConcurrentHashMap<>();
        jobBlockTime = config.getLongVar(ConfVars.JOB_BLOCK_TIME);
        connInterval = config.getLongVar(ConfVars.TDMANAGER_HEART_INTERV);
        updateZkInterval = config.getIntVar(ConfVars.DEFAULT_UPDATE_POSITION_INTERVAL);
        resettingCheckInterval = config.getIntVar(ConfVars.RESETTING_CHECK_INTERVAL);
    }

    public static HttpProxySender getProxySender() {
        return proxySender;
    }

    @Override
    public void start() {
        submitWorker(heartBeatAndAlarmThread());
        submitWorker(getJobResettingCheckTask());
        submitWorker(getPositionUpdateTask());
    }

    public void initMonitor(String jobName, long hbTimeStample, DBSyncJob job) {
        LOGGER.info("Monitor job add {}", jobName);
        monitorJobs.put(jobName, job);
        jobHb.put(jobName, hbTimeStample);
    }

    public void removeJobMonitor(String jobName) {
        LOGGER.info("remove job monitor {}", jobName);
        monitorJobs.remove(jobName);
        jobHb.remove(jobName);
        jobSenderPosition.remove(jobName);
    }

    public void markToStop(String jobName) {
        //to mark old job has been stopped.
        DBSyncJob job = monitorJobs.get(jobName);
        JSONObject stopHeartbeat = getHeartBeatInfo(jobName, job, true);
        if (stopHeartbeat != null) {
            LOGGER.info(jobName + " mark to stop.");
            holdHeartbeats.offer(stopHeartbeat);
        }
    }

    public JSONObject getLastHbInfo(String jobName) {
        DBSyncJob job = monitorJobs.get(jobName);
        LOGGER.info("all monitor jobs {}", monitorJobs.keySet());
        return getHeartBeatInfo(jobName, job, true);
    }

    public void putStopHeartbeat(JSONObject stopHb) {
        holdHeartbeats.offer(stopHb);
        LOGGER.debug("stopHb: " + stopHb);
    }

    public void doSwitchMonitor(String newJobName, String oldJobName) {
        LOGGER.info("switch monitor new {}, old {}", newJobName, oldJobName);
        DBSyncJob job = monitorJobs.remove(oldJobName);
        monitorJobs.put(newJobName, job);

        Long hbTimeStample = jobHb.get(oldJobName);
        jobHb.put(newJobName, hbTimeStample);

        jobSenderPosition.remove(oldJobName);
    }

    public void updateJobSenderPosition(String jobName, LogPosition jobPosition) {
        jobSenderPosition.put(jobName, jobPosition);
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    private Runnable heartBeatAndAlarmThread() {
        return () -> {
            while (isRunnable() || sInfos.size() > 0) {
                try {
                    // heartBeat here is used for OP to monitor status
                    JSONObject hearBeat = new JSONObject();
                    hearBeat.put("ip", config.getLocalIp());
                    JSONArray hbInfos = new JSONArray();

                    if (!holdHeartbeats.isEmpty()) {
                        JSONObject stopHbInfo = null;
                        do {
                            try {
                                stopHbInfo = holdHeartbeats.poll(1, TimeUnit.MILLISECONDS);
                                if (stopHbInfo != null) {
                                    hbInfos.add(stopHbInfo);
                                }
                            } catch (Exception e) {
                                LOGGER.error("Get Hold Heartbeat exception: ", e);
                                break;
                            }
                        } while (stopHbInfo != null);
                    }

                    for (String jobName : monitorJobs.keySet()) {
                        JSONObject hbInfo = getHeartBeatInfo(jobName, monitorJobs.get(jobName));
                        if (hbInfo != null) {
                            hbInfos.add(hbInfo);
                        }
                    }
                    hearBeat.put("heartbeat", hbInfos);

                    //deal statistic info
                    StatisticInfo sInfo = null;
                    JSONArray statisticInfos = new JSONArray();
                    do {
                        // !!!should sort by datetime
                        sInfo = sInfos.peek();

                        if (sInfo == null) {
                            LOGGER.info("there's no sInfo, break");
                            break;
                        }

                        AtomicLong messageValue = new AtomicLong(0);
                        statisticData.compute(sInfo.key, (k, v) -> {
                            if (v != null) {
                                messageValue.set(v.longValue());
                            }
                            sInfos.poll();
                            return null;
                        });

                        AtomicLong packageValue = new AtomicLong(0);
                        statisticDataPackage.compute(sInfo.key, (k, v) -> {
                            if (v != null) {
                                packageValue.set(v.longValue());
                            }
                            return null;
                        });
                        LogPosition position = jobNewestPosition.get(sInfo.getInstName());
                        LOGGER.info("job Position: {}, Job instant name {}", position.getJsonObj(),
                                sInfo.getInstName());

                        JSONObject statInfo =
                                getStatInfoAndSendToBus(sInfo, messageValue.get(), packageValue.get(),
                                        position);
                        if (statInfo != null) {
                            statisticInfos.add(statInfo);
                        }
                    } while (sInfo != null);

                    // to cope with new tdm, no statistic_info should be
                    hearBeat.put("statistic_info", statisticInfos);

                    //upload jobs status
                    String hburl = config.getStringVar(ConfVars.TDMANAGER_HEARTBEAT_URL);
                    String token = config.getStringVar(ConfVars.TDMANAGER_AUTH_TOKEN);
                    String serviceName = config.getStringVar(ConfVars.TDMANAGER_SERVICE_NAME);

                    try {
                        JSONObject params = new JSONObject();
                        params.put("heartbeat", hbInfos);
                        params.put("ip", config.getLocalIp());
                        LOGGER.debug("HeartBeat Info : {}", params);
                        TDManagerConn.cgi2TDManager(hburl, params, token, serviceName);

                        if (DBSyncBuffer.getInstance().needWait()) {
                            LOGGER.error(" Send data to pulsar Blocking");
                        }

                    } catch (Throwable t) {
                        LOGGER.error("Send heartbeat msg Error : ", t);
                    }

                    //send job alarm
                    while (!alarmMsg.isEmpty()) {
                        JobAlarmMsg msg = alarmMsg.poll();
                        DBSyncJob job = monitorJobs.get(msg.jobName);
                        if (msg != null && job != null) {
                            String alarmParson = job.getJobConf().getAlarmPerson();
                            if (alarmParson == null) {
                                continue;
                            }
                            LOGGER.error(msg.msg);
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error("Monitor thread has exception e = {}", e);
                } finally {
                    long interval = connInterval + ((long) (random.nextFloat() * connInterval / 2));
                    try {
                        if (running) {
                            TimeUnit.MILLISECONDS.sleep(interval);
                        }
                    } catch (InterruptedException e) {
                    }
                }
            }
            metricPulsarSender.stop();
            bStoped = true;//TODO:need?
        };
    }

    private Runnable getJobResettingCheckTask() {
        return () -> {
            while (isRunnable()) {
                try {
                    for (Map.Entry<String, DBSyncJob> runningJob : jobManager.getRunningJobs().entrySet()) {
                        DBSyncJob job = runningJob.getValue();
                        DBSyncJobConf jobConf =
                                jobConfManager.getParsingConfigByInstName(runningJob.getKey()).getDbSyncJobConf();
                        if (jobConf != null) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("current mysql Ip = {}/{} ,contain = {}",
                                        jobConf.getCurMysqlIp(), jobConf.getCurMysqlPort(),
                                        jobConf.containsDatabase(jobConf.getCurMysqlUrl()));
                            }
                            if (job != null && (!jobConf.containsDatabase(jobConf.getCurMysqlUrl()))) {
                                if (jobConf.getStatus() == JobStat.TaskStat.SWITCHING) {
                                    LOGGER.warn("Job [{}] is switching!, so skip for resetting!",
                                            runningJob.getKey());
                                } else {
                                    CompletableFuture<Void> future = job.resetJob();
                                    if (future != null) {
                                        future.whenCompleteAsync((ign, t) -> {
                                            if (t != null) {
                                                LOGGER.error("job[{}] has exception while resetting:",
                                                        runningJob.getKey(), t);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                    TimeUnit.SECONDS.sleep(resettingCheckInterval);
                } catch (Throwable e) {
                    LOGGER.error("getJobResettingCheckTask has exception ", e);
                }
            }
        };
    }

    private Runnable getPositionUpdateTask() {
        return () -> {
            while (isRunnable()) {
                try {
                    /*
                     * When there is no table name or field name matching rule,
                     *  it needs to be updated with the position parsed by binlog
                     */
                    for (Map.Entry<String, DBSyncJob> runningJob : jobManager.getRunningJobs().entrySet()) {
                        if (!runningJob.getValue().isRunning()) {
                            LOGGER.warn("runningJob [{}] is not running!", runningJob.getKey());
                            continue;
                        }
                        LogPosition storePos = getStorePosition(runningJob.getKey(),
                                runningJob.getValue());
                        if (storePos == null) {
                            LOGGER.warn("runningJob [{}] store position is null", runningJob.getKey());
                            continue;
                        }
                        LogPosition lastStorePosition =
                                jobManager.getRunningJobsLastStorePositionMap().get(runningJob.getKey());
                        if (lastStorePosition != null && lastStorePosition.equals(storePos)) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("position has stored, jobName = {} ,position = {}",
                                        runningJob.getKey(), lastStorePosition);
                            }
                            continue;
                        }
                        updatePosition(runningJob.getKey(), storePos,
                                runningJob.getValue().getPkgIndexId());
                        jobManager.getRunningJobsLastStorePositionMap().put(runningJob.getKey(), storePos);
                    }
                    TimeUnit.SECONDS.sleep(updateZkInterval);
                } catch (Throwable e) {
                    LOGGER.error("getPositionUpdateTask has exception ", e);
                }

            }
        };
    }

    private void updatePosition(String jobName, LogPosition ackedPos, long pkgIndexId) {
        JSONObject jsonObject = ackedPos.getJsonObj();
        jsonObject.put(MSG_INDEX_KEY, pkgIndexId);
        updateJobSenderPosition(jobName, ackedPos);
        jobHaDispatcher.updatePosition(getSyncIdFromInstanceName(jobName),
                jsonObject.toJSONString());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("updatePosition jobName = {} pkgIndexId = {} position = {} ", jobName,
                    pkgIndexId, ackedPos);
        }
    }

    private String getSyncIdFromInstanceName(String key) {
        if (key != null) {
            String[] splitA = key.split(":");
            if (splitA.length >= 3) {
                return splitA[2];
            }
        }
        return null;
    }

    public void addStatisticInfo(String key, String bid, String intfName, long timeStample,
            long msgCnt, String topic, String oldKey, LogPosition latestLogPosition,
            String instName, long serverId) {
        statisticData.compute(key, (k, v) -> {
            if (v == null) {
                try {
                    sInfos.put(new StatisticInfo(bid, intfName, timeStample, topic, key, oldKey,
                            latestLogPosition, instName, serverId));
                } catch (InterruptedException e) {
                    LOGGER.error("put {}#{} StatisticInfo {} error, ", bid, intfName, timeStample,
                            e);
                }
                return new AtomicLong(msgCnt);
            } else {
                v.addAndGet(msgCnt);
                return v;
            }
        });

        statisticDataPackage.compute(key, (k, v) -> {
            if (v == null) {
                return new AtomicLong(1);
            } else {
                v.addAndGet(1);
                return v;
            }
        });

    }

    public void addRecodeLogPosition(PackageData pkgData) {
        jobManager.addRecodeLogPosition(pkgData);
    }

    private JSONObject getHeartBeatInfo(String jobName, DBSyncJob job) {
        return getHeartBeatInfo(jobName, job, false);
    }

    private JSONObject getHeartBeatInfo(String jobName, DBSyncJob job, boolean markToStop) {
        if (jobName == null || job == null) {
            LOGGER.error("jobName is {} or job is {}", jobName, job);
            return null;
        }

        long nowTime = System.currentTimeMillis();

        if (job.getJobStat() == JobStat.State.RUN || markToStop) {
            JSONObject hbInfo = new JSONObject();
            hbInfo.put("serverId", job.getJobConf().getServerId());
            hbInfo.put("taskIds", job.getJobConf().getTasksJson());
            if (markToStop) {
                hbInfo.put("agentStatus", "STOPPED");
            } else {
                hbInfo.put("agentStatus", job.getJobConf().getStatus().name());
            }
            hbInfo.put("currentDb", job.getJobConf().getCurMysqlIp());
            hbInfo.put("dbIp", job.getJobConf().getCurMysqlIp());
            hbInfo.put("dbPort", job.getJobConf().getCurMysqlPort());
            hbInfo.put("dbDumpIndex", job.getPkgIndexId());

            if (job.getJobConf().getBakMysqlIp() != null) {
                hbInfo.put("backupDbIp", job.getJobConf().getBakMysqlIp());
                hbInfo.put("backupDbPort", job.getJobConf().getBakMysqlPort());
            }

            LogPosition jobPosition = jobSenderPosition.get(jobName);
            if (jobPosition != null) {
                hbInfo.put("dumpPosition", jobPosition.getJsonObj());
            }

            LogPosition sendPosition = getStorePositionFirstFromCache(jobName, job);
            if (sendPosition == null) {
                sendPosition = job.getJobConf().getStartPos();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("jobName = {} position = {}"
                            + "get sendPosition from start position!", jobName, sendPosition);
                }
            }
            if (sendPosition != null) {
                hbInfo.put("sendPosition", sendPosition.getJsonObj());
            }
            LogPosition maxLogPosition = job.getMaxLogPosition();
            /**
             * max log position null when db cannot connect
             * maxLogPosition set to parsePosition when such situation happens
             */
            if (maxLogPosition == null) {
                maxLogPosition = job.getLogPosition();
            }
            recordMaxPosition(jobName, maxLogPosition);
            sendMaxPositionRecord(job, maxLogPosition, sendPosition);

            long newHB = job.getHearBeat();
            LogPosition tmpLogPos = job.getLogPosition();
            if (tmpLogPos != null) {
                if (tmpLogPos.getPosition().getTimestamp() != null) {
                    //Unboxing of null Long may produce 'java.lang.NullPointerException'
                    long dumpTime = tmpLogPos.getPosition().getTimestamp();
                    Long lastDiffTime = sendedAlartMap.get(jobName);
                    long nowDiffTime = nowTime - dumpTime;
                    if (lastDiffTime != null && nowDiffTime > jobBlockTime &&
                            nowDiffTime > lastDiffTime) {

                        LogPosition maxLogPos = maxLogPosition;

                        if (maxLogPos == null) {
                            // need send alarm
                            String msg =
                                    job.getJobConf().getJobName() + " may be Blocking, Msg from " +
                                            config.getLocalIp() + "!\n" + "now time : " + nowTime +
                                            " , position time : " + dumpTime;
                            LOGGER.error(
                                    "Detected job :{} may be blocking, last HeartBeat time {}, " +
                                            "now {}. msg {}",
                                    jobName, newHB, nowTime, msg);
                        } else {
                            hbInfo.put("maxLogPosition", maxLogPos.getJsonObj());
                            String maxFileName = maxLogPos.getPosition().getJournalName();
                            String logFileName = tmpLogPos.getPosition().getJournalName();
                            long maxPos = maxLogPos.getPosition().getPosition();
                            long logPos = tmpLogPos.getPosition().getPosition();
                            if (!maxFileName.equalsIgnoreCase(logFileName) ||
                                    Math.abs(maxPos - logPos) > 1000) {
                                // need send alarm
                                String msg = job.getJobConf().getJobName() +
                                        " may be Blocking, Msg from " + config.getLocalIp() +
                                        "!\n" + "now time : " + nowTime + " , position time : " +
                                        dumpTime;
                                LOGGER.error(
                                        "Detected job :{} may be blocking, last HeartBeat time " +
                                                "{}, now {}, msg {}",
                                        jobName, newHB, nowTime, msg);
                            }
                        }
                    }

                    sendedAlartMap.put(jobName, nowDiffTime);
                    jobHb.put(jobName, newHB);
                }
            }
            return hbInfo;
        } else {
            LOGGER.error("Detected job :{} is in wrong state :{}", jobName, job.getJobStat());
            if (monitorFlag && (job.getJobStat() == JobStat.State.STOP ||
                    job.getJobStat() == JobStat.State.INIT)) {
                //use job Manager start the job
                LOGGER.error("restart job : {}", jobName);
                job.restart();
            }
            JSONObject hbInfo = new JSONObject();
            hbInfo.put("serverId", job.getJobConf().getServerId());
            hbInfo.put("dbIp", job.getJobConf().getCurMysqlIp());
            hbInfo.put("dbPort", job.getJobConf().getCurMysqlPort());

            hbInfo.put("currentDb", job.getJobConf().getCurMysqlIp());

            if (job.getJobConf().getBakMysqlIp() != null) {
                hbInfo.put("backupDbIp", job.getJobConf().getBakMysqlIp());
                hbInfo.put("backupDbPort", job.getJobConf().getBakMysqlPort());
            }

            hbInfo.put("taskIds", job.getJobConf().getTasksJson());
            hbInfo.put("errorMsg", job.getErrorMsg());
            return hbInfo;
        }
    }

    public LogPosition getStorePositionFirstFromCache(String jobName, DBSyncJob job) {
        if (job == null) {
            return null;
        }
        LogPosition storePos = null;
        if (StringUtils.isEmpty(jobName)) {
            if (job.getJobConf() != null) {
                jobName = job.getJobConf().getJobName();
            }
        }
        if (StringUtils.isNotEmpty(jobName)) {
            storePos = jobManager.getRunningJobsLastStorePositionMap().get(jobName);
        }
        if (storePos == null) {
            storePos = getStorePosition(jobName, job);
        }
        return storePos;
    }

    //TODO:解耦
    public LogPosition getStorePosition(String jobName, DBSyncJob job) {
        if (job == null) {
            return null;
        }
        LogPosition storePos = null;
        if (StringUtils.isEmpty(jobName)) {
            if (job.getJobConf() != null) {
                jobName = job.getJobConf().getJobName();
            }
        }
        if (storePos == null) {
            storePos = job.getMinCacheSendLogPosition();
            if (storePos == null) {
                storePos = job.getMinCacheEventLogPosition();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] getLogPosition = {}", jobName, storePos);
                }
                LogPosition minPos = job.getMinCacheSendLogPosition();
                if (minPos != null) {
                    storePos = minPos;
                }
            }
        }
        return storePos;
    }


    /**
     * save the max position to reduce the network cost
     *
     * @param jobName
     * @param maxLogPos
     */
    private void recordMaxPosition(String jobName, LogPosition maxLogPos) {
        if (maxLogPos == null) {
            LOGGER.warn("record max log position, jobName {}, maxLogPos is null", jobName);
            return;
        }
        LOGGER.info("record max log position, jobName {}, maxLogPos {}", jobName,
                maxLogPos.getJsonObj());
        jobNewestPosition.put(jobName, maxLogPos);
    }


    /**
     * for every task in a server we should record max log position periodically since sometimes we
     * have no data in some task and we should still record the position
     *
     * @param job
     * @param maxLogPosition
     */
    private void sendMaxPositionRecord(DBSyncJob job, LogPosition maxLogPosition,
            LogPosition sendPosition) {
        List<Integer> taskIdList = job.getDBSyncJobConf().getTaskIdList();
        for (Integer taskId : taskIdList) {
            MysqlTableConf mysqlTableConf = job.getDBSyncJobConf().getMysqlTableConf(taskId);
            StatisticInfo info =
                    new StatisticInfo(mysqlTableConf.getGroupId(), mysqlTableConf.getStreamId(),
                            System.currentTimeMillis(), sendPosition,
                            job.getDBSyncJobConf().getJobName(),
                            Long.parseLong(job.getDBSyncJobConf().getServerId()));
            sendMetricsToPulsar(info, 0, maxLogPosition);
        }
    }

    private JSONObject getStatInfoAndSendToBus(StatisticInfo sInfo, Long cnt, Long packageCnt,
            LogPosition newestPosition) {
        if (cnt != null && packageCnt != null) {
            JSONObject obj = new JSONObject();
            obj.put("topic", sInfo.getBid());
            obj.put("intf_name", sInfo.getIntfName());
            obj.put("time_stamp", sInfo.getTimeStample());
            obj.put("packageCnt", packageCnt);
            obj.put("linecnt", cnt);
            LOGGER.info("Static info {} : {}", sInfo.oldKey, cnt);
            sendMetricToBus(sInfo.getBid(), sInfo.getIntfName(), sInfo.getTopic(),
                    sInfo.getTimeStample(), packageCnt, cnt);
            sendMetricsToPulsar(sInfo, cnt, newestPosition);
            return obj;
        }
        return null;
    }

    private void sendMetricToBus(String bid, String interfaceName, String topic, long timeStamp,
            long packageCnt, long lineCnt) {
        StringJoiner joiner = new StringJoiner(BUS_DELIMITER);
        String message = joiner.add(config.getLocalIp()).add(topic).add(String.valueOf(timeStamp)).
                add(bid).add(interfaceName).add(String.valueOf(lineCnt))
                .add(String.valueOf(packageCnt)).toString();
        ProxyEvent event = new ProxyEvent(message, metricBid, metricTid, System.currentTimeMillis(), 20 * 1000);
        BusMessageQueueCallback callback = new BusMessageQueueCallback(event);
        HttpProxySender busSender = JobMonitor.getProxySender();
        busSender.asyncSendMessage(message, metricBid, metricTid, System.currentTimeMillis(),
                20 * 1000, TimeUnit.MILLISECONDS, callback);
    }


    private void sendMetricsToPulsar(StatisticInfo sInfo, long lineCnt,
            LogPosition newestPosition) {
        Long newestPositionPos = 0L;
        String newestPositionBinlogName = "";
        if (newestPosition == null || newestPosition.getPosition() == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("serverId:{} bid:{} tid:{} instName:{} newestPosition is null!",
                        sInfo.getServerId(), sInfo.getBid(),
                        sInfo.getIntfName(), sInfo.getInstName());
            }
        } else {
            newestPositionPos = newestPosition.getPosition().getPosition();
            newestPositionBinlogName = newestPosition.getPosition().getJournalName();
        }
        LogPosition logPosition = sInfo.getLatestLogPosition();
        Long latestLogPositionPos = 0L;
        Long latestLogPositionTimestamp = 0L;
        String latestLogPositionBinlogName = "";
        String latestPositionAddr = "";
        if (logPosition == null || logPosition.getPosition() == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("serverId:{} bid:{} tid:{} instName:{} no current position",
                        sInfo.getServerId(), sInfo.getBid(),
                        sInfo.getIntfName(), sInfo.getInstName());
            }
        } else {
            latestLogPositionPos = logPosition.getPosition().getPosition();
            latestLogPositionTimestamp = logPosition.getPosition().getTimestamp();
            latestLogPositionBinlogName = logPosition.getPosition().getJournalName();
            if (logPosition.getIdentity() != null && logPosition.getIdentity().getSourceAddress()
                    != null) {
                latestPositionAddr =
                        logPosition.getIdentity().getSourceAddress().getAddress().getHostAddress();
            }
        }
        JobMetricInfo info =
                JobMetricInfo.builder().bid(sInfo.getBid()).dataTime(sInfo.getTimeStample())
                        .cnt(lineCnt).dbCurrentTime(latestLogPositionTimestamp)
                        .dbCurrentPosition(latestLogPositionPos)
                        .dbCurrentBinlog(latestLogPositionBinlogName)
                        .dbIp(latestPositionAddr)
                        .dbNewestPosition(newestPositionPos)
                        .dbNewestBinlog(newestPositionBinlogName)
                        .idx(generateSnowId(sInfo.getServerId())).ip(config.getLocalIp())
                        .reportTime(System.currentTimeMillis()).tid(sInfo.getIntfName())
                        .serverId(sInfo.getServerId()).build();
        LOGGER.info("send metrics to pulsar as record {}", info);
        metricPulsarSender.sendMetrics(info);
    }


    /**
     * generate snow id using serverId;
     *
     * @param serverId
     */
    private long generateSnowId(long serverId) {
        idGeneratorMap.computeIfAbsent(serverId, k -> new SnowFlake(serverId));
        return idGeneratorMap.get(serverId).nextId();
    }

    /**
     * generate uuid for the metric to be unique
     *
     * @return
     */
    private String getUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public void resetMonitorFlag() {
        monitorFlag = false;
    }

    public void stopMonitor() {
        running = false;
        while (!bStoped) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    public void jobSendAlarm(String jobName, String msg) {
        alarmMsg.offer(new JobAlarmMsg(jobName, msg));
    }

    private static class StatisticInfo {

        private LogPosition latestLogPosition;
        private String key;
        private String oldKey;
        private String bid;
        private String intfName;
        private String topic;
        private long timeStample;
        private long serverId;
        private String instName;

        public StatisticInfo(String bid, String intfName, long timeStample, String topic,
                String key, String oldKey, LogPosition latestLogPosition, String instName,
                long serverId) {
            this.bid = bid;
            this.intfName = intfName;
            this.topic = topic;
            this.timeStample = timeStample;
            this.key = key;
            this.latestLogPosition = latestLogPosition;
            this.instName = instName;
            this.serverId = serverId;
            this.oldKey = oldKey;
        }

        public StatisticInfo(String businessId, String intfName, long timeStample,
                LogPosition logPosition, String instName, long serverId) {
            this.bid = businessId;
            this.intfName = intfName;
            this.instName = instName;
            this.timeStample = timeStample;
            this.latestLogPosition = logPosition;
            this.serverId = serverId;
        }

        public LogPosition getLatestLogPosition() {
            return latestLogPosition;
        }

        public long getServerId() {
            return serverId;
        }

        public long getTimeStample() {
            return timeStample;
        }

        public String getTopic() {
            return topic;
        }

        public String getBid() {
            return bid;
        }

        public String getIntfName() {
            return intfName;
        }

        public String getInstName() {
            return instName;
        }

    }

    private static class JobAlarmMsg {

        String jobName;
        String msg;

        public JobAlarmMsg(String jobName, String msg) {
            this.jobName = jobName;
            this.msg = msg;
        }
    }
}
