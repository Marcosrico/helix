package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PuppyManager {
  private List<AbstractPuppy> puppies;
  private ExecutorService executorService;

  public PuppyManager() {
    puppies = new ArrayList<>();
    executorService = Executors.newCachedThreadPool();
  }

  public void addPuppy(AbstractPuppy puppy) {
    puppies.add(puppy);
  }

  public void start(long timeoutInSeconds) {
    for (AbstractPuppy puppy : puppies) {
      executorService.submit(puppy);
    }

    try {
      executorService.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    stop();
  }

  public void stop() {
    executorService.shutdownNow();
  }
}

