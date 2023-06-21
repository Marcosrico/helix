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

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;

import java.util.Objects;
import java.util.Random;

public class GetPuppy extends AbstractPuppy {

  public GetPuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    super(metaclient, puppySpec);
  }

  @Override
  protected void bark() throws Exception {
    int random = new Random().nextInt(puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      // Intentional error
      try {
        metaclient.get("invalid");
      } catch (IllegalArgumentException e) {
        System.out.println(Thread.currentThread().getName() + " intentionally tried to read an invalid path.");
      }
    } else {
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to read node: " + random);
        if (Objects.equals(metaclient.get("/test/" + random), null)) {
          System.out.println(Thread.currentThread().getName() + " failed to read node " + random + ", it does not exist");
        } else {
          System.out.println(
              Thread.currentThread().getName() + " successfully read node " + random + " at time: "
                  + System.currentTimeMillis());
          eventChangeCounter++;
        }
      } catch (MetaClientNoNodeException e) {
        System.out.println(Thread.currentThread().getName() + " failed to read node " + random + ", it does not exist");
      } catch (IllegalArgumentException e) {
        if (!e.getMessage().equals("Can not subscribe one time watcher when ZkClient is using PersistWatcher")) {
          throw e;
        }
      }
    }
  }
  @Override
  protected void cleanup() {
    metaclient.recursiveDelete("/test");
  }

  private boolean shouldIntroduceError() {
    Random random = new Random();
    float randomValue = random.nextFloat();
    return randomValue < puppySpec.getErrorRate();
  }
}
