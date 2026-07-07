/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.bigtable.jetstream.tools.commands;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.bigtable.v2.OpenTableRequest.Permission;
import com.google.bigtable.v2.SessionReadRowRequest;
import com.google.cloud.bigtable.data.v2.internal.api.Client;
import com.google.cloud.bigtable.data.v2.internal.api.ClientSettings;
import com.google.cloud.bigtable.data.v2.internal.api.TableAsync;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Resource;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.cloud.bigtable.jetstream.tools.util.ByteStringOptionConverter;
import com.google.cloud.bigtable.jetstream.tools.util.Metrics;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/*
Example:

run.sh generate-load \
 --mode directpath \
 --endpoint test-bigtable.sandbox.googleapis.com \
 --app-profile-id default \
 --table-name projects/google.com:cloud-bigtable-dev/instances/clusterpin-bidi-tm/tables/ycsb \
 --key user1000000000876325578
*/
@Command(
    name = "generate-load",
    description = "Generate steady qps",
    sortOptions = false,
    sortSynopsis = false,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class GenerateLoad implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateLoad.class);

  @Mixin private Target target;
  @Mixin private Resource resource;

  enum LoadType {
    READ_ROW(Permission.PERMISSION_READ),
    MUTATE_ROW(Permission.PERMISSION_WRITE),
    PING(Permission.PERMISSION_READ);

    final Permission permission;

    LoadType(Permission permission) {
      this.permission = permission;
    }
  }

  @Option(names = "--load-type")
  private LoadType loadType = LoadType.READ_ROW;

  @Option(
      names = "--key",
      description = "row key as a string",
      converter = ByteStringOptionConverter.class)
  private ByteString[] rowKeys = {ByteString.copyFromUtf8("0"),
                                  ByteString.copyFromUtf8("1"),
                                  ByteString.copyFromUtf8("2"),
                                  ByteString.copyFromUtf8("3"),
                                  ByteString.copyFromUtf8("4"),
                                  ByteString.copyFromUtf8("5"),
                                  ByteString.copyFromUtf8("6"),
                                  ByteString.copyFromUtf8("7"),
                                  ByteString.copyFromUtf8("8"),
                                  ByteString.copyFromUtf8("9"),
                                  ByteString.copyFromUtf8("a"),
                                  ByteString.copyFromUtf8("b"),
                                  ByteString.copyFromUtf8("c"),
                                  ByteString.copyFromUtf8("d"),
                                  ByteString.copyFromUtf8("e"),
                                  ByteString.copyFromUtf8("f"),
                                  ByteString.copyFromUtf8("g"),
                                  ByteString.copyFromUtf8("h"),
                                  ByteString.copyFromUtf8("i"),
                                  ByteString.copyFromUtf8("j"),
                                  ByteString.copyFromUtf8("k"),
                                  ByteString.copyFromUtf8("l"),
                                  ByteString.copyFromUtf8("m"),
                                  ByteString.copyFromUtf8("n"),
                                  ByteString.copyFromUtf8("o"),
                                  ByteString.copyFromUtf8("p"),
                                  ByteString.copyFromUtf8("q"),
                                  ByteString.copyFromUtf8("r"),
                                  ByteString.copyFromUtf8("s"),
                                  ByteString.copyFromUtf8("t"),
                                  ByteString.copyFromUtf8("u"),
                                  ByteString.copyFromUtf8("v"),
                                  ByteString.copyFromUtf8("w"),
                                  ByteString.copyFromUtf8("x"),
                                  ByteString.copyFromUtf8("y"),
                                  ByteString.copyFromUtf8("z")
  };

  @Option(names = "--concurrency", description = "number of concurrent reads")
  private int concurrency = 10;

  @Option(names = "--target-qps", description = "overall qps across all worker threads")
  private int qps = 4_000;

  @Option(names = "--payload-size", description = "The request size")
  private int payloadSize = 0;

  private ByteString payload;
  private Metrics metrics;
  private Random random = new Random();

  private static class StatsTracker {
    final long[] latencies;
    final AtomicInteger count = new AtomicInteger();
    final long startTime = System.nanoTime();

    StatsTracker(int capacity) {
      latencies = new long[capacity];
    }

    void record(long latencyMs) {
      int idx = count.getAndIncrement();
      if (idx < latencies.length) {
        latencies[idx] = latencyMs;
      }
    }
  }

  @Override
  public Void call() throws Exception {
    final AtomicReference<StatsTracker> currentTracker = new AtomicReference<>(new StatsTracker(Math.max(100_000, qps * 20)));
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {
      StatsTracker oldTracker = currentTracker.getAndSet(new StatsTracker(Math.max(100_000, qps * 20)));
      int count = oldTracker.count.get();
      int validCount = Math.min(count, oldTracker.latencies.length);
      long[] lats = new long[validCount];
      System.arraycopy(oldTracker.latencies, 0, lats, 0, validCount);
      Arrays.sort(lats);

      long elapsedNanos = System.nanoTime() - oldTracker.startTime;
      double elapsedSecs = elapsedNanos / 1e9;
      double rps = validCount / elapsedSecs;
      double fraction = rps * concurrency / qps;

      long p50 = validCount > 0 ? lats[(int)(validCount * 0.50)] : 0;
      long p90 = validCount > 0 ? lats[(int)(validCount * 0.90)] : 0;
      long p95 = validCount > 0 ? lats[(int)(validCount * 0.95)] : 0;
      long p99 = validCount > 0 ? lats[(int)(validCount * 0.99)] : 0;
      long p999 = validCount > 0 ? lats[(int)(validCount * 0.999)] : 0;

      System.out.printf("RPS: %.2f, Fraction of expected: %.4f, p50: %d ms, p90: %d ms, p95: %d ms, p99: %d ms, p99.9: %d ms%n",
          rps, fraction, p50, p90, p95, p99, p999);
    }, 10, 10, TimeUnit.SECONDS);

    byte[] payloadData = new byte[payloadSize];
    new Random().nextBytes(payloadData);
    payload = ByteString.copyFrom(payloadData);

    Client client =
        Client.create(
            ClientSettings.builder()
                .setChannelProvider(target.getChannelProvider())
                .setInstanceName(resource.getTableName().getInstanceName())
                .setAppProfileId(resource.getAppProfileId())
                .build());

    TableAsync table =
        client.openTableAsync(resource.getTableName().getTableId(), Permission.PERMISSION_READ);

    this.metrics =
        Metrics.create(
            loadType.name().toLowerCase() + "_bidi2",
            GoogleCredentials.getApplicationDefault(),
            target,
            resource.getTableName());

    Semaphore semaphore = new Semaphore(concurrency);
    RateLimiter limiter = RateLimiter.create(qps);

    while (true) {
      limiter.acquire();
      semaphore.acquire();

      Stopwatch stopwatch = Stopwatch.createStarted();
      CompletableFuture<?> f = null;
      switch (loadType) {
        case READ_ROW:
          {
            ByteString randomRowKey = rowKeys[random.nextInt(rowKeys.length)];
            f =
                table.readRow(
                    SessionReadRowRequest.newBuilder().setKey(randomRowKey).build(),
                    Deadline.after(1, TimeUnit.MINUTES));
            break;
          }
        default:
          throw new UnsupportedOperationException("Unsupported load type: " + loadType);
      }

      f.whenComplete(
          (result, throwable) -> {
            long latencyMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (throwable != null) {
              System.err.println("Future completed with an error: " + throwable.getMessage());
            } else {
              StatsTracker tracker = currentTracker.get();
              if (tracker != null) {
                tracker.record(latencyMs);
              }
            }
            metrics.recordLatency(
                Duration.of(stopwatch.elapsed(TimeUnit.MICROSECONDS), ChronoUnit.MICROS),
                resource.getAppProfileId());
            semaphore.release();
          });
    }
  }
}
