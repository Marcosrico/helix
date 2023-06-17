package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

/**
 * PuppySpec class definition
 */
public class PuppySpec {
  private final PuppyMode mode;
  private final float errorRate;
  private final ExecDelay execDelay;
  private final ExecDelay recoverDelay;

  public PuppySpec(PuppyMode mode, float errorRate, ExecDelay execDelay, ExecDelay recoverDelay) {
    this.mode = mode;
    this.errorRate = errorRate;
    this.execDelay = execDelay;
    this.recoverDelay = recoverDelay;
  }

  public PuppyMode getMode() {
    return mode;
  }

  public float getErrorRate() {
    return errorRate;
  }

  public ExecDelay getExecDelay() {
    return execDelay;
  }

  public ExecDelay getRecoverDelay() {
    return recoverDelay;
  }
}
