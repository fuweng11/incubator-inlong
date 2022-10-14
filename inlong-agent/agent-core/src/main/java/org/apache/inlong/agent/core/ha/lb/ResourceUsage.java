package org.apache.inlong.agent.core.ha.lb;

/**
 * 功能描述：job ha
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 12:02
 * @Description:
 */
public class ResourceUsage {

    public double usage = 0D;

    public double limit = 0D;

    public ResourceUsage(double usage, double limit) {
        this.usage = usage;
        this.limit = limit;
    }

    public ResourceUsage(ResourceUsage that) {
        this.usage = that.usage;
        this.limit = that.limit;
    }

    public ResourceUsage() {
    }

    public void reset() {
        this.usage = -1;
        this.limit = -1;
    }

    /**
     * this may be wrong since we are comparing available and not the usage.
     *
     * @param o
     * @return
     */
    public int compareTo(ResourceUsage o) {
        double required = o.limit - o.usage;
        double available = limit - usage;
        return Double.compare(available, required);
    }

    public float percentUsage() {
        float proportion = 0;
        if (limit > 0) {
            proportion = ((float) usage) / ((float) limit);
        }
        return proportion;
    }
}
