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

import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.DirectChildChangeListener;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;

import java.util.Random;

public class DirectChildListenerPuppy extends AbstractPuppy {

  public DirectChildListenerPuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    super(metaclient, puppySpec);
  }

  @Override
  protected void bark() {
    int random = new Random().nextInt(puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      // Intentional error
      try {
        DirectChildChangeListener listener = new DirectChildChangeListener() {
          @Override
          public void handleDirectChildChange(String key) throws Exception {
            ;
          }
        };
        metaclient.subscribeDirectChildChange("invalid", listener, false);
      } catch (IllegalArgumentException e) {
        System.out.println(Thread.currentThread().getName() + " intentionally set a direct child listener on an invalid path.");
      }
    } else {
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to set a direct child listener on node test");
        DirectChildChangeListener listener = new DirectChildChangeListener() {
          @Override
          public void handleDirectChildChange(String key) throws Exception {
            ;
          }
        };
        metaclient.subscribeDirectChildChange("/test", listener, false);
        System.out.println(
            Thread.currentThread().getName() + " successfully set a direct child listener on node test "
                + System.currentTimeMillis());
      } catch (MetaClientNoNodeException e) {
        System.out.println(Thread.currentThread().getName() + " failed to set a direct child listener on test, it does not exist");
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