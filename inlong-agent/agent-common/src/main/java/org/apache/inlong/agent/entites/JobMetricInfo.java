package org.apache.inlong.agent.entites;

import lombok.Builder;
import lombok.Data;

/**
 * @Author pengzirui
 * @Date 2021/12/29 9:42 下午
 * @Version 1.0
 */
@Data
@Builder
public class JobMetricInfo {

    private String ip;
    private long cnt;
    private long dataTime;
    private long reportTime;
    private String dbIp;
    private long dbNewestPosition;
    private String dbNewestBinlog;
    private long dbCurrentTime;
    private long dbCurrentPosition;
    private String dbCurrentBinlog;
    private String bid;
    private String tid;
    private long idx;
    private long serverId;

}
