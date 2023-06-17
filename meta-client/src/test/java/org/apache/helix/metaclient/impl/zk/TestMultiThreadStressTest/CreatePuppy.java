package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.zookeeper.KeeperException;

import java.util.Random;

public class CreatePuppy extends AbstractPuppy {

  public CreatePuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    super(metaclient, puppySpec);
  }

  @Override
  protected void bark() throws Exception {
    // Implement the chaos logic for creating nodes
    int random = new Random().nextInt(20);
    if (shouldIntroduceError()) {
      try {
        // Simulate an error by creating an invalid path
        metaclient.create("invalid", "test");
      } catch (MetaClientException e) {
        System.out.println(Thread.currentThread().getName() + " tried to create an invalid path.");
        // Expected exception
      }
    } else {
      // Normal behavior - create a new node
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to create node: " + random);
        metaclient.create("/test/" + random,"test");
        System.out.println(Thread.currentThread().getName() + " successfully created node " + random + " at time: " + System.currentTimeMillis());
      } catch (MetaClientException e) {
        // Expected exception
        System.out.println(Thread.currentThread().getName() + " failed to create node " + random + ", it already exists");
      }
    }
  }

  @Override
  protected void cleanup() {
    // Implement the recovery logic by deleting the created documents
    metaclient.recursiveDelete("/test");
  }

  private boolean shouldIntroduceError() {
    Random random = new Random();
    float randomValue = random.nextFloat();
    return randomValue < puppySpec.getErrorRate();
  }
}


