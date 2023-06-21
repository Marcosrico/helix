package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.metaclient.impl.zk.ZkMetaClient;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
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
    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 20);
    _zkMetaClient.create("/test", "test");
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    GetPuppy getPuppy = new GetPuppy(_zkMetaClient, puppySpec);
    DeletePuppy deletePuppy = new DeletePuppy(_zkMetaClient, puppySpec);
    SetPuppy setPuppy = new SetPuppy(_zkMetaClient, puppySpec);
    UpdatePuppy updatePuppy = new UpdatePuppy(_zkMetaClient, puppySpec);
    ListenerPuppy listenerPuppy = new ListenerPuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(getPuppy);
    puppyManager.addPuppy(deletePuppy);
    puppyManager.addPuppy(setPuppy);
    puppyManager.addPuppy(updatePuppy);
    puppyManager.addPuppy(listenerPuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    // Assert no unhandled (unexpected) exceptions
    for (AbstractPuppy puppy : puppyManager.getPuppies()) {
      Assert.assertEquals(puppy.unhandledErrorCounter, 0);
    }

  }
}
