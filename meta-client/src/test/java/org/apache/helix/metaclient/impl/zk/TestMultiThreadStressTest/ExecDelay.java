package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

import java.util.Random;

/**
 * ExecDelay class definition
 */
public class ExecDelay {
  private final long duration;
  private final float jitter;

  private final long delayBase;
  private final long delayRange;

  public ExecDelay(long duration, float jitter) {
    if (jitter < 0 || jitter > 1 || duration < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid jitter (%s) or duration (%s)", jitter, duration));
    }
    this.duration = duration;
    this.jitter = jitter;
    this.delayRange = Math.round(this.duration * this.jitter * 2);
    this.delayBase = this.duration - this.delayRange / 2;
  }

  /**
   * Calculate the next delay based on the configured duration and jitter.
   * @return The next delay in milliseconds.
   */
  public long getNextDelay() {
    Random random = new Random();
    long randomDelay = this.delayBase + random.nextLong() % this.delayRange;
    return Math.max(randomDelay, 0);
  }

  public long getDuration() {
    return duration;
  }

  public float getJitter() {
    return jitter;
  }
}
