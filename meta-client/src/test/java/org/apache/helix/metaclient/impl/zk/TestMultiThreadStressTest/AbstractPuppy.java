package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

import org.apache.helix.metaclient.api.MetaClientInterface;

import java.util.Random;

import java.util.Random;

/**
 * AbstractPuppy object contains interfaces to implement puppy and main logics to manage puppy life cycle
 */
public abstract class AbstractPuppy implements Runnable {

  protected MetaClientInterface<String> metaclient;
  protected PuppySpec puppySpec;

  public AbstractPuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    this.metaclient = metaclient;
    this.puppySpec = puppySpec;
  }

  /**
   * Implements puppy's main logic. Puppy needs to implement its chaos logic, recovery logic based on
   * errorRate, recoverDelay. For OneOff puppy, it will bark once with execDelay in spec, and for
   * Repeat puppy, it will bark forever, with execDelay between 2 barks
   */
  protected abstract void bark() throws Exception;

  /**
   * Implements puppy's final cleanup logic - it will be called only once right before the puppy terminates.
   * Before the puppy terminates, it needs to recover from all chaos it created.
   */
  protected abstract void cleanup();

  @Override
  public void run() {
    try {
      while (true) {
        if (puppySpec.getMode() == PuppyMode.OneOff) {
          bark();
          cleanup();
          break;
        } else {
          bark();
          Thread.sleep(puppySpec.getExecDelay().getNextDelay());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}


