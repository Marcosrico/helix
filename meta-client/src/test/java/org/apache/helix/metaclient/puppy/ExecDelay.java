package org.apache.helix.metaclient.puppy;

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

import java.util.Random;

/**
 * ExecDelay class definition
 */
public class ExecDelay {
  private final long _duration;
  private final float _jitter;

  private final long _delayBase;
  private final long _delayRange;
  private Random _random;

  public ExecDelay(long duration, float jitter) {
    if (jitter < 0 || jitter > 1 || duration < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid _jitter (%s) or _duration (%s)", jitter, duration));
    }
    this._duration = duration;
    this._jitter = jitter;
    this._delayRange = Math.round(this._duration * this._jitter * 2);
    this._delayBase = this._duration - this._delayRange / 2;
  }

  /**
   * Calculate the next delay based on the configured _duration and _jitter.
   * @return The next delay in milliseconds.
   */
  public long getNextDelay() {
    _random = new Random();
    long randomDelay = this._delayBase + _random.nextLong() % this._delayRange;
    return Math.max(randomDelay, 0);
  }

  public long getDuration() {
    return _duration;
  }

  public float getJitter() {
    return _jitter;
  }
}
