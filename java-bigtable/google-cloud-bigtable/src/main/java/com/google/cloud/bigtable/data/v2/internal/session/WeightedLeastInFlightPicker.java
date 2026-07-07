/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.data.v2.internal.session;

import com.google.bigtable.v2.LoadBalancingOptions;
import com.google.cloud.bigtable.data.v2.internal.session.SessionList.AfeHandle;
import com.google.cloud.bigtable.data.v2.internal.session.SessionList.SessionHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class WeightedLeastInFlightPicker extends Picker {

  private static final Logger DEFAULT_LOGGER = Logger.getLogger(WeightedLeastInFlightPicker.class.getName());
  private static final AtomicLong lastWarningTimeMillis = new AtomicLong(0);
  private Logger logger = DEFAULT_LOGGER;

  private final SessionList sessionList;

  public WeightedLeastInFlightPicker(SessionList sessionList) {
    this.sessionList = sessionList;
  }

  @Override
  Optional<SessionHandle> pickSession() {
    List<AfeHandle> readyAfes = sessionList.getAfesWithReadySessions();

    if (readyAfes.isEmpty()) {
      logger.warning("No AFEs available for request");
      return Optional.empty();
    }

    ThreadLocalRandom rng = ThreadLocalRandom.current();
    // Weight is from 0 to 100, and indicates likelihood that a candidate should be picked.
    List<AfeHandle> candidates = new ArrayList<>(readyAfes.stream().filter(afe -> afe.weight > 0 && (afe.weight >= 100 || rng.nextInt(100) <= afe.weight)).collect(Collectors.toList()));
    if (candidates.isEmpty()) {
      long now = System.currentTimeMillis();
      long last = lastWarningTimeMillis.get();
      if (now - last > 100 && lastWarningTimeMillis.compareAndSet(last, now)) {
        logger.warning("All candidate AFEs have 0 weight, picking at random");
      }
      // TODO: maybe we can be smarter about weighting by cost or something.
      return sessionList.checkoutSession(readyAfes.get(rng.nextInt(readyAfes.size())));
    }

    double bestCost = Double.MAX_VALUE;
    AfeHandle bestAfe = null;
    long iterations = candidates.size();

    // Find AFE with the best cost, addressing them in a random order so that if multiple AFEs have the same cost we pick one at random.
    // Partial Fisher-Yates shuffle.
    for (int i = 0; i < iterations; i++) {
      int randomIndex = i + rng.nextInt(candidates.size() - i);
      AfeHandle picked = candidates.get(randomIndex);
      double cost = picked.getNumOutstanding() / (double)picked.weight;
      if (cost < bestCost) {
        bestCost = cost;
        bestAfe = picked;
      }
      // Move candidate to the `i`th entry so that it's not picked again.
      Collections.swap(candidates, i, randomIndex);
    }
    return sessionList.checkoutSession(bestAfe);
  }
}
