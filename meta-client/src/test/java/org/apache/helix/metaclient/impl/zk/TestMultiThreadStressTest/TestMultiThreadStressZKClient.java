package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

import org.apache.helix.metaclient.impl.zk.ZkMetaClient;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestMultiThreadStressZKClient extends ZkMetaClientTestBase {

  private ZkMetaClient<String> _zkMetaClient;

  @BeforeTest
  private void setUp() {
    this._zkMetaClient = createZkMetaClient();
    this._zkMetaClient.connect();
  }

  @Test
  public void testMultiThreadStressZKClient() throws Exception {
    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), new ExecDelay(3000, 0.2f));
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    _zkMetaClient.create("/test", "test");

    Thread puppyThread = new Thread(createPuppy);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);

    // Add more puppies as needed

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    // Stop the puppy thread when you're done
    puppyThread.join();
  }
}
