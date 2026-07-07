/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.data.v2.internal.session;

import static com.google.bigtable.v2.CloseSessionRequest.CloseSessionReason.CLOSE_SESSION_REASON_MISSED_HEARTBEAT;

import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.CloseSessionRequest;
import com.google.bigtable.v2.PeerLoadInfo;
import com.google.bigtable.v2.PeerInfo;
import com.google.cloud.bigtable.data.v2.internal.middleware.VRpc.VRpcResult;
import com.google.cloud.bigtable.data.v2.internal.session.Session.SessionState;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.Comparator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data structure to track sessions through their lifecycle transitions.
 *
 * <p>Each session will be wrapped in a SessionHandle to track per Session state.
 *
 * <p>This class is not thread safe and requires external synchronization.
 */
@NotThreadSafe
class SessionList {
  private static final Logger LOG = Logger.getLogger(SessionList.class.getName());

  static final Duration SESSION_LIST_PRUNE_INTERVAL = Duration.ofMinutes(1);

  // List of afes that have sessions that are ready now.
  private final List<AfeHandle> afesWithReadySessions = new ArrayList<>();
  // A map of all recently used Afes with possibly empty sessions
  private final Map<AfeId, AfeHandle> afeHandles = new HashMap<>();
  // All the sessions being tracked by this SessionList including:
  // - ones that are in use
  // - starting sessions
  // - closing sessions
  private final Set<SessionHandle> allSessions = new HashSet<>();
  private final Set<SessionHandle> inUseSessions = new HashSet<>();

  private final CloseSessionRequest missedHeartbeatCloseRequest =
      CloseSessionRequest.newBuilder()
          .setReason(CLOSE_SESSION_REASON_MISSED_HEARTBEAT)
          .setDescription("missed heartbeat")
          .build();

  // pool level statistics across all  the afes
  private final PoolStats poolStats = new PoolStats();

  private long lastWeightUpdateNs = 0;

  /** Entrypoint for a session's lifecycle */
  SessionHandle newHandle(Session session) {
    SessionHandle h = new SessionHandle(session);
    allSessions.add(h);
    poolStats.startingCount++;
    poolStats.expectedCapacity++;
    return h;
  }

  /** Get {@link PoolStats} */
  PoolStats getStats() {
    return poolStats;
  }

  Set<SessionHandle> getAllSessions() {
    return allSessions;
  }

  List<AfeHandle> getAfesWithReadySessions() {
    return Collections.unmodifiableList(afesWithReadySessions);
  }

  /**
   * Gets the next ready session from the afe. This will be called when a vrpc is about to be
   * started, it is called by the {@link Picker}.
   */
  public Optional<SessionHandle> checkoutSession(AfeHandle afeHandle) {
    Optional<SessionHandle> maybeHandle = Optional.ofNullable(afeHandle.sessions.poll());

    maybeHandle.ifPresent(
        handle -> {
          poolStats.readyCount--;
          poolStats.inUseCount++;
          inUseSessions.add(handle);
          if (handle.afe.get().sessions.isEmpty()) {
            afesWithReadySessions.remove(afeHandle);
          }
        });

    return maybeHandle;
  }

  /** Closes all the sessions with this reason. */
  void close(CloseSessionRequest req) {
    // Notify all sessions to close and have the callbacks clean up the rest of the state
    for (SessionHandle s : allSessions) {
      s.getSession().close(req);
    }
  }

  void prune() {
    Instant now = Instant.now();
    Instant horizon = now.minus(SESSION_LIST_PRUNE_INTERVAL);

    Iterator<Entry<AfeId, AfeHandle>> it = afeHandles.entrySet().iterator();
    while (it.hasNext()) {
      Entry<AfeId, AfeHandle> e = it.next();
      AfeHandle handle = e.getValue();
      if (handle.refCount > 0) {
        continue;
      }
      if (handle.lastConnected.isBefore(horizon)) {
        it.remove();
      }
    }
  }


  /**
   * Maps a collection of keys to their values. Empty optional values are replaced with 
   * the average of all present values. If all values are empty, the provided default value is used.
   * 
   * @param keys The collection of keys to process.
   * @param valueExtractor A function mapping a key to an Optional numeric value.
   * @param defaultValue The fallback value to use if all extracted values are empty.
   * @return A Map from key to their corresponding Double value.
   */
  private static <K> Map<K, Double> mapWithAverage(
          Collection<K> keys, 
          Function<K, Optional<Double>> valueExtractor,
          double defaultValue) {

      Map<K, Optional<Double>> tempMap = new LinkedHashMap<>();
      double sum = 0.0;
      long count = 0;

      for (K key : keys) {
          Optional<Double> optVal = valueExtractor.apply(key);
          tempMap.put(key, optVal);
          if (optVal != null && optVal.isPresent()) {
              sum += optVal.get().doubleValue();
              count++;
          }
      }

      double fallbackValue = (count > 0) ? (sum / count) : defaultValue;

      Map<K, Double> resultMap = new LinkedHashMap<>();
      for (Map.Entry<K, Optional<Double>> entry : tempMap.entrySet()) {
          double finalValue = entry.getValue() != null && entry.getValue().isPresent()
                  ? entry.getValue().get().doubleValue()
                  : fallbackValue;
          resultMap.put(entry.getKey(), finalValue);
      }

      return resultMap;
  }

  // TODO: also call this when convergeance time changes.
  void updateWeights() {
    // TODO: find AFEs and sessions that are ready
    if (afeHandles.isEmpty()) {
      LOG.warning("no AFEs, skipping weight update.");
      return;
    }

    Map<AfeHandle, AfeId> handleToId = new HashMap<>();
    for (Map.Entry<AfeId, AfeHandle> entry : afeHandles.entrySet()) {
      handleToId.put(entry.getValue(), entry.getKey());
    }

    Map<AfeHandle, Double> afesByNetworkLatency = mapWithAverage(afeHandles.values(), afe -> afe.transportLatency.samples > 10 ? Optional.of(afe.getTransportCost() / 1_000_000.0) : Optional.<Double>empty(), 0.5);

    // Identify most common AFE load version, if they're all the same, pick the highest version.
    Optional<String> optionalPreferredVersion = afeHandles.values().stream()
    .flatMap(afe -> afe.afeLoad.getVersionedLoadInfoMap().keySet().stream())
    .collect(Collectors.groupingBy(v -> v, Collectors.counting()))
    .entrySet().stream()
    .max(Map.Entry.<String, Long>comparingByValue()
      .thenComparing(Map.Entry::getKey))
    .map(Map.Entry::getKey);

    if (!optionalPreferredVersion.isPresent()) {
      LOG.warning("No load data for any AFE, skipping weight update.");
      return;
    }
    String preferredVersion = optionalPreferredVersion.get();

    double maxWeight = afeHandles.values().stream().mapToDouble(afe -> afe.weight).max().orElse(100.0);

    // TODO: find out what is causing cost of NaN, and also make this code more resilient to NaN.
    // Higher cost --> less desirable.
    Map<AfeHandle, Double> afesByCost = mapWithAverage(afeHandles.values(), afe -> {
              if (!afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion)) {
                LOG.warning(String.format("Unexpected condition: AFE %d missing preferred version %s", handleToId.get(afe).getId(), preferredVersion));
                return Optional.<Double>empty();
              }
              PeerLoadInfo.VersionedPeerInfo loadInfo = afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion);

              // Prefer to use the same set of AFEs where possible
              double alreadyPickedBonus = 1.0;
              if (maxWeight > 0 && !Double.isNaN(maxWeight)) {
                alreadyPickedBonus = 1.0 + 0.5 * afe.weight / (double) maxWeight;
              }
              double expectedTotalLatency = loadInfo.getExpectedLatencyMs() + afesByNetworkLatency.get(afe);

              return Optional.of(expectedTotalLatency / alreadyPickedBonus / (loadInfo.getWeight() < 0.00001 ? 1.0 : loadInfo.getWeight()));
            }, 100 // we don't expect to ever use the default value
          );
    // Find the AFEs with the lowest cost.
    List<AfeHandle> sortedAfes = new ArrayList<>(afeHandles.values());
    sortedAfes.sort(Comparator.comparingDouble(afe -> afesByCost.get(afe)));

    // TODO: have AFEs publish their current RIF and utilization, and allow clients to compute potential RIF.
    // General formula should be to first find target utilization, which is max(80, p75 AFE utilization),
    // find available RIF, then reduce it using a constant multiplier to try to avoid overshooting.
    // TODO: also look at number of sessions, which also limits RIF.
    Map<AfeHandle, Double> afesByPotentialRif = mapWithAverage(afeHandles.values(), afe -> {
              if (!afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion)) {
                return Optional.<Double>empty();
              }

              PeerLoadInfo.VersionedPeerInfo loadInfo = afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion);
              return Optional.of((double) (Math.max(0, Math.min(
                // RIFs that the server can handle + RIFs that the server is already handling from this client.
                loadInfo.getAvailableRif() + afe.getNumOutstanding(),
                // RIFs that the client can send to this server, i.e. number of sessions (because sessions aren't multiplexed).
                afe.refCount))));
    }, 1);

    double maxRif = Math.max(5, afesByPotentialRif.values().stream().mapToDouble(v -> v).sum());
    double outstandingRif = afeHandles.values().stream().mapToDouble(afe -> afe.getNumOutstanding()).sum();
    // If possible, pick enough AFEs to handle a 50% increase in load.
    double expectedRif = outstandingRif * 1.5;
    String derivedFrom = "outstandingRif * 1.5";
    if (expectedRif > maxRif * 0.6) {
      // If we don't have enough sessions to handle the expected load, pick enough to handle a 10% load increase OR 60% of maximum possible load.
      // This helps to avoid picking all available AFEs.
      expectedRif = Math.max(maxRif * 0.6, outstandingRif * 1.1);
      derivedFrom = expectedRif  == maxRif * 0.6 ?  "maxRif * 0.6" : "outstandingRif * 1.1";
    }
    expectedRif = Math.ceil(expectedRif);

    LOG.info(String.format("expected RIF: %.2f, max possible RIF: %.2f, in-flight RIF: %.2f. Expected is derived from: %s", expectedRif, maxRif, outstandingRif, derivedFrom));

    double rifRemaining = expectedRif;
    int minAfesLeftToPick = 2;

    long now = System.nanoTime();
    long ms_since_last_update = Math.min(1_000, Math.max(0, TimeUnit.NANOSECONDS.toMillis(now - lastWeightUpdateNs)));
    // Round to nearest 10ms to try to keep weight updates nice even numbers.
    ms_since_last_update = Math.round(ms_since_last_update / 10.0) * 10;
    if (ms_since_last_update > 0) {
      lastWeightUpdateNs = now;
    }

    // Find the maximum weight among the AFEs that are actually going to be picked
    double maxPickedWeight = 0;
    double tempRifRemaining = expectedRif;
    int tempMinAfesLeft = 2;
    for (AfeHandle afe : sortedAfes) {
      if ((tempRifRemaining > 0 || tempMinAfesLeft > 0) && afe.refCount > 0 && (!afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion) || afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion).getAvailableRif() > 0)) {
        maxPickedWeight = Math.max(maxPickedWeight, afe.weight);
        tempRifRemaining -= afesByPotentialRif.get(afe);
        tempMinAfesLeft--;
      }
    }

    // Pick the fewest AFEs that are able to handle the expected load, preferring those with the lowest cost.
    // Increase the weight of AFEs that were picked, and decrease the weight of those that weren't.
    int numWeight100 = 0;
    int numWeight0 = 0;
    int numWeightInBetween = 0;
    List<Long> idsWeight100 = new ArrayList<>();
    List<Long> idsWeightInBetween = new ArrayList<>();

    for (AfeHandle afe : sortedAfes) {
      int delta_weight = 10;
      if (afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion)) {
        delta_weight = (int) Math.round(100.0 * ms_since_last_update / Math.max(1, afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion).getConvergeanceTimeMs() == 0 ? 1000 : afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion).getConvergeanceTimeMs()));
      }

      int oldWeight = afe.weight;

      if ((rifRemaining > 0 || minAfesLeftToPick > 0) && afe.refCount > 0 && (!afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion) || afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion).getAvailableRif() > 0)) {
        // Ensure that at least one AFE gets a weight of 100.
        delta_weight = (int) Math.max(delta_weight, 100 - maxPickedWeight);
        // Pick AFE to use, it's a good one.
        // We want to continue sending traffic to this AFE.
        rifRemaining -= afesByPotentialRif.get(afe);
        minAfesLeftToPick--;
        afe.weight = Math.min(100, afe.weight + delta_weight);
      } else {
        // We want to reduce traffic to this AFE.
        afe.weight = Math.max(0, afe.weight - delta_weight);
      }

      long id = handleToId.get(afe).getId();

      if (afe.weight == 100) {
        numWeight100++;
        idsWeight100.add(id);
      } else if (afe.weight == 0) {
        numWeight0++;
      } else {
        numWeightInBetween++;
        idsWeightInBetween.add(id);
      }

      // if (oldWeight != afe.weight || afe.weight == 100 || afe.weight == 1) {
      double potentialRif = afesByPotentialRif.get(afe);
      int outstandingRif2 = afe.getNumOutstanding();
      double cost = afesByCost.get(afe);
      
      double expectedLatency = 0.0;
      double peerLoadWeight = 1.0;
      if (afe.afeLoad.getVersionedLoadInfoMap().containsKey(preferredVersion)) {
        PeerLoadInfo.VersionedPeerInfo loadInfo = afe.afeLoad.getVersionedLoadInfoMap().get(preferredVersion);
        expectedLatency = loadInfo.getExpectedLatencyMs();
        peerLoadWeight = loadInfo.getWeight();
      }

      double networkLatency = afesByNetworkLatency.get(afe);
      double totalLatency = expectedLatency + networkLatency;
      double latencyPerWeight = totalLatency / (peerLoadWeight < 0.00001 ? 1.0 : peerLoadWeight);

      LOG.info(String.format("AFE %d: weight changed from %d to %d, potential rif: %.2f, outstanding rif: %d, num sessions: %d, cost: %.2f, expected latency: %.2f, network latency: %.2f, expected+network latency: %.2f, peer load weight: %.2f, (expected+network latency)/weight: %.2f", 
          id, oldWeight, afe.weight, potentialRif, outstandingRif2, afe.refCount, cost, expectedLatency, networkLatency, totalLatency, peerLoadWeight, latencyPerWeight));
      // }
    }
    double usableSessions = afeHandles.values().stream().mapToDouble(afe -> afe.refCount * afe.weight / 100.0).sum();
    double totalSessions = afeHandles.values().stream().mapToDouble(afe -> afe.refCount).sum();
    // Assumes no multiplexing.
    poolStats.usableFraction = usableSessions / totalSessions;
    LOG.info(String.format("%d pct of streams are useable out of %d total", (int) (poolStats.usableFraction * 100), (int) totalSessions));

    LOG.info(String.format("AFE weights: %d with weight 100 (ids: %s), %d with weight 0, %d with weight in-between (ids: %s)", numWeight100, idsWeight100, numWeight0, numWeightInBetween, idsWeightInBetween));
  }

  void checkHeartbeat(Clock clock) {
    Instant now = clock.instant();
    inUseSessions.forEach(
        handle -> {
          if (now.isAfter(handle.getSession().getNextHeartbeat())) {
            LOG.log(
                Level.WARNING,
                "Missed heartbeat for {0}, forcing session close",
                handle.getSession().getLogName());
            handle.getSession().forceClose(missedHeartbeatCloseRequest);
          }
        });
  }

  @NotThreadSafe
  class SessionHandle {
    private final Session session;
    private boolean inExpectedCount = true;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<AfeHandle> afe = Optional.empty();

    SessionHandle(Session session) {
      this.session = session;
    }

    Session getSession() {
      return session;
    }

    /** First transition in the happy path - server acknowledged the session */
    void onSessionStarted() {
      PeerInfo peerInfo = session.getPeerInfo();

      AfeHandle afeHandle =
          afeHandles.computeIfAbsent(AfeId.extract(peerInfo), (ignored) -> new AfeHandle());
      this.afe = Optional.of(afeHandle);
      afeHandle.sessions.add(this);
      afeHandle.refCount++;
      afeHandle.lastConnected = Instant.now();
      if (afeHandle.sessions.size() == 1) {
        afesWithReadySessions.add(afeHandle);
      }
      afeHandle.afeLoad = peerInfo.getLoadInfo();

      poolStats.startingCount--;
      poolStats.readyCount++;
    }

    /**
     * The session is returned to the pool after use. This undoes what SessionList#checkoutSession
     */
    void onVRpcFinish(Duration elapsed, VRpcResult result) {
      // Guaranteed to be set - vrpc can only start after the session is ready
      AfeHandle afeHandle = this.afe.get();

      poolStats.inUseCount--;
      inUseSessions.remove(this);

      if (result.getStatus().isOk()) {
        afeHandle.updateLatency(elapsed, result.getBackendLatency());
      }

      if (session.getState() == SessionState.READY) {
        poolStats.readyCount++;
        afeHandle.sessions.add(this);
        afeHandle.lastConnected = Instant.now();

        // If this is the first session returned to the pool, transition the afe to ready list
        if (afeHandle.sessions.size() == 1) {
          afesWithReadySessions.add(afeHandle);
        }
      }
    }

    /**
     * Server started graceful refresh. The session is still available, but a replacement is being
     * searched for.
     */
    void onSessionClosing() {
      // The session could get a goaway before it started, which means it has not been
      // associated with an afe.
      // Also the session could get a goaway when its either idle or in use.
      boolean wasReady = false;

      // if afe is not present, the session has not started, so skip this
      if (afe.isPresent()) {
        wasReady = afe.get().sessions.remove(this);

        if (afe.get().sessions.isEmpty()) {
          afesWithReadySessions.remove(afe.get());
        }
      }

      if (wasReady) {
        poolStats.readyCount--;
      }
      // eagerly decrement expected count and make sure to avoid double counting in onSessionClosed
      poolStats.expectedCapacity--;
      inExpectedCount = false;
    }

    void onSessionClosed(SessionState prevState) {
      if (inExpectedCount) {
        poolStats.expectedCapacity--;
        inExpectedCount = false;
      }
      // only update counts after the session started and has an afe associated
      afe.ifPresent(afeHandle -> afeHandle.refCount--);

      // NOTE: don't need to update vRpc counters, onVRpcFinish will have been invoked already
      switch (prevState) {
        case NEW:
          throw new IllegalStateException("NEW session was closed");
        case STARTING:
          poolStats.startingCount--;
          break;
        case READY:
          {
            AfeHandle afeHandle = afe.get();
            // If the session was available & idle, then we need to remove it
            if (afeHandle.sessions.remove(this)) {
              poolStats.readyCount--;
              if (afeHandle.sessions.isEmpty()) {
                afesWithReadySessions.remove(afeHandle);
              }
            }
            break;
          }
        case CLOSING:
        case WAIT_SERVER_CLOSE:
          // noop
          break;
        case CLOSED:
          throw new IllegalStateException("double close");
      }

      allSessions.remove(this);
    }

    void onPeerLoad(PeerLoadInfo peerLoad) {
      afe.ifPresent(afeHandle -> afeHandle.afeLoad = peerLoad);
    }
  }

  /** Simple counters for the sessions contained in this list. */
  @NotThreadSafe
  static class PoolStats {
    private int startingCount;
    private int readyCount;
    private int inUseCount;
    private int expectedCapacity;
    private double usableFraction;

    PoolStats() {
      reset();
    }

    void reset() {
      startingCount = 0;
      readyCount = 0;
      inUseCount = 0;
      expectedCapacity = 0;
      usableFraction = 1.0;
    }

    /** Number of Sessions that are being prepped, but not ready for use. */
    int getStartingCount() {
      return startingCount;
    }

    /** Number of Sessions ready for immediate use. */
    int getReadyCount() {
      return readyCount;
    }

    /**
     * Number of Sessions that are in use and thus unavailable. This includes sessions that will not
     * be returned to the pool because they will be closed when the go idle.
     */
    int getInUseCount() {
      return inUseCount;
    }

    /**
     * Number of sessions should be usable in the short term future. Includes sessions that are
     * starting, idle and in used.
     */
    int getExpectedCapacity() {
      return expectedCapacity;
    }

    double getUsableFraction() {
      return usableFraction;
    }

    @VisibleForTesting
    TestHelper getTestHelper() {
      return new TestHelper();
    }

    @Override
    public String toString() {
      return String.format(
          "PoolStats{startingCount=%d, readyCount=%d, inUseCount=%d, expectedCapacity=%d, usableFraction=%d}",
          startingCount, readyCount, inUseCount, expectedCapacity, (int)(usableFraction * 100));
    }

    @VisibleForTesting
    class TestHelper {
      void setStartingCount(int n) {
        startingCount = n;
      }

      void setReadyCount(int n) {
        readyCount = n;
      }

      void setExpectedCapacity(int n) {
        expectedCapacity = n;
      }

      void setInUseCount(int n) {
        inUseCount = n;
      }
    }
  }

  /** Typesafe wrapper around the applicationFrontendId the server sent us */
  @AutoValue
  abstract static class AfeId {
    protected abstract long getId();

    static AfeId extract(PeerInfo peerInfo) {
      return new AutoValue_SessionList_AfeId(peerInfo.getApplicationFrontendId());
    }
  }

  static class AfeHandle {
    // All sessions in the queue are ready to be used
    @VisibleForTesting final Queue<SessionHandle> sessions;
    // Last time this afe was used. It will be consulted when we need to garbage collect
    // afe that have disappeared
    private Instant lastConnected = Instant.now();
    // Tracks number ready and inUse sessions, also used for garbage collection
    // TODO: expose via method
    int refCount = 0;

    private final PeakEwma transportLatency = new PeakEwma(Duration.of(500, ChronoUnit.MICROS));
    private final PeakEwma e2eLatency = new PeakEwma(Duration.ofMillis(1));

    PeerLoadInfo afeLoad = PeerLoadInfo.getDefaultInstance();
    // 0-100.
    int weight = 50;

    public AfeHandle() {
      sessions = new ArrayDeque<>();
    }

    void updateLatency(Duration e2eLatency, Duration backendLatency) {
      this.transportLatency.update(e2eLatency.minus(backendLatency));
      this.e2eLatency.update(e2eLatency);
    }

    double getTransportCost() {
      return transportLatency.getCost();
    }

    double getE2eCost() {
      return e2eLatency.getCost();
    }

    int getNumOutstanding() {
      return refCount - sessions.size();
    }
  }

  static class PeakEwma {
    // Use the last 10s as a look back window
    private final double decayNs = TimeUnit.SECONDS.toNanos(10);
    private long timestamp = System.nanoTime();
    private double cost;
    private long samples = 0;

    public PeakEwma(Duration initialLatency) {
      this.cost = initialLatency.toNanos();
    }

    public double getCost() {
      return cost;
    }

    void update(Duration rtt) {
      if (rtt.compareTo(Duration.ZERO) <= 0) {
        LOG.warning("Ignoring latency<= 0: " + rtt);
        return;
      }

      this.samples++;

      long now = System.nanoTime();
      long rttNs = rtt.toNanos();

      if (cost < rttNs) {
        this.cost = rttNs;
      } else {
        long elapsed = Math.max(now - timestamp, 0);
        double decay = Math.exp(-elapsed / decayNs);
        double recency = 1.0 - decay;
        this.cost = cost * decay + rttNs * recency;
      }
      this.timestamp = now;
    }
  }
}
