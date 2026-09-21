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

import com.google.cloud.bigtable.data.v2.internal.session.SessionList.AfeHandle;
import com.google.cloud.bigtable.data.v2.internal.session.SessionList.SessionHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class ConsistentHashingWithBoundedLoadPicker extends Picker {

  private static final Logger DEFAULT_LOGGER =
      Logger.getLogger(ConsistentHashingWithBoundedLoadPicker.class.getName());
  private static final AtomicLong lastWarningTimeMillis = new AtomicLong(0);
  private Logger logger = DEFAULT_LOGGER;

  private final SessionList sessionList;

  public ConsistentHashingWithBoundedLoadPicker(SessionList sessionList) {
    this.sessionList = sessionList;
  }

  @Override
  Optional<SessionHandle> pickSession() {
    List<AfeHandle> readyAfes = new ArrayList<>(sessionList.getAfesWithReadySessions());

    if (readyAfes.isEmpty()) {
      logger.warning("No AFEs available for request");
      return Optional.empty();
    }

    // There's only one table, so we'll be lazy and assume that the has function for that table is the id function.
    readyAfes.sort(Comparator.comparingLong(afe -> afe.afeId.getId()));

    for (AfeHandle afe : readyAfes) {
      if (afe.afeLoad != null && afe.afeLoad.getUtilization() < 0.85) {
        return sessionList.checkoutSession(afe);
      }
    }

    logger.info("No AFEs have utilization < 85%, picking the least loaded one.");

    AfeHandle bestAfe = readyAfes.get(0);
    double leastLoad = bestAfe.afeLoad != null ? bestAfe.afeLoad.getUtilization() : 0.0;

    for (int i = 1; i < readyAfes.size(); i++) {
      AfeHandle current = readyAfes.get(i);
      double currentLoad = current.afeLoad != null ? current.afeLoad.getUtilization() : 0.0;
      if (currentLoad < leastLoad) {
        leastLoad = currentLoad;
        bestAfe = current;
      }
    }

    return sessionList.checkoutSession(bestAfe);
  }
}
