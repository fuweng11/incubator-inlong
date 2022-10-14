package org.apache.inlong.agent.core.ha.lb;

import lombok.Getter;
import lombok.Setter;

/**
 * 功能描述：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 14:49
 * @Description:
 */
@Getter
@Setter
public class SystemResourceUsage {

    public ResourceUsage bandwidthIn;

    public ResourceUsage bandwidthOut;

    public ResourceUsage cpu;

    public ResourceUsage systemMemory;
}
