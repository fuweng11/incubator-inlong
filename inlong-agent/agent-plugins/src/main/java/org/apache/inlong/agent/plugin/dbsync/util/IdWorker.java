package org.apache.inlong.agent.plugin.dbsync.util;

/**
 * 
 * 来自于twitter项目<a
 * href="https://github.com/twitter/snowflake">snowflake</a>的id产生方案，全局唯一，时间有序
 * 
 */
public class IdWorker {
  private final long workerId;
  private final static long twepoch = 1303895660503L;
  private long sequence = 0L;
  private final static long workerIdBits = 10L;
  private final static long maxWorkerId = -1L ^ -1L << workerIdBits;
  private final static long sequenceBits = 12L;

  private final static long workerIdShift = sequenceBits;
  private final static long timestampLeftShift = sequenceBits + workerIdBits;
  private final static long sequenceMask = -1L ^ -1L << sequenceBits;

  private long lastTimestamp = -1L;

  public IdWorker(final long workerId) {
    super();
    if (workerId > maxWorkerId || workerId < 0) {
      throw new IllegalArgumentException(String.format(
          "worker Id can't be greater than %d or less than 0", maxWorkerId));
    }
    this.workerId = workerId;
  }

  public synchronized long nextId() {
    long timestamp = this.timeGen();
    if (this.lastTimestamp == timestamp) {
      this.sequence = this.sequence + 1 & sequenceMask;
      if (this.sequence == 0) {
        timestamp = this.tilNextMillis(this.lastTimestamp);
      }
    } else {
      this.sequence = 0;
    }

    this.lastTimestamp = timestamp;

    long generatedId = timestamp - twepoch << timestampLeftShift
        | this.workerId << workerIdShift | this.sequence;
    if (generatedId < 0)
      generatedId = -generatedId;
    return generatedId;
  }

  private long tilNextMillis(final long lastTimestamp) {
    long timestamp = this.timeGen();
    while (timestamp <= lastTimestamp) {
      timestamp = this.timeGen();
    }
    return timestamp;
  }

  private long timeGen() {
    return System.nanoTime();
  }
}