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
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.BigtableStub;
import com.google.bigtable.v2.FeatureFlags;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.DeleteFromRow;
import com.google.bigtable.v2.PingAndWarmRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.data.v2.internal.api.Util;
import com.google.cloud.bigtable.jetstream.tools.commands.args.MultiResource;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.cloud.bigtable.jetstream.tools.core.IpInterceptor;
import com.google.cloud.bigtable.jetstream.tools.util.Metrics;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.CallOptions.Key;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
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
@SuppressWarnings("UnstableApiUsage")
@Command(
    name = "generate-unary-load",
    description = "Generate steady qps using classic unary apis",
    sortOptions = false,
    sortSynopsis = false,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class GenerateUnaryLoad implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateUnaryLoad.class);

  @Mixin private Target target;
  @Mixin private MultiResource resource;

  enum LoadType {
    READ_ROW,
    MUTATE_ROW,
    PING;
  }

  @Option(names = "--load-type")
  private LoadType loadType = LoadType.READ_ROW;

  @Option(names = "--key", description = "row key as a string")
  private String rowKey = "user1000000000876325578";

  @Option(names = "--concurrency", description = "number of concurrent reads")
  private int concurrency = 10;

  @Option(names = "--target-qps", description = "overall qps across all worker threads")
  private int qps = 4_000;

  private Metrics metrics;

  @Override
  public Void call() throws Exception {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    CallCredentials callCreds = MoreCallCredentials.from(credentials);
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    this.metrics =
        Metrics.create(
            loadType.name().toLowerCase() + "_unary", credentials, target, resource.getTableName());

    double perWorkerQps = (double) qps / (double) concurrency;
    List<ByteString> rowKeys = ImmutableList.of(ByteString.copyFromUtf8(rowKey));

    for (int i = 0; i < concurrency; i++) {
      ManagedChannel channel =
          target
              .getChannelProvider()
              .newChannelBuilder(callCreds)
              .intercept(new IpInterceptor())
              .intercept(new MetadataInterceptor())
              .build();
      executor.submit(new Worker(channel, callCreds, RateLimiter.create(perWorkerQps), rowKeys));
    }

    boolean ignored = executor.awaitTermination(365, TimeUnit.DAYS);
    return null;
  }

  class Worker implements Callable<Void> {
    private final Random random = new Random();

    private final RateLimiter rateLimiter;
    private final List<String> appProfileIds;
    private final List<ByteString> rowKeys;

    private final BigtableStub stub;
    private final String instanceName;

    Worker(
        ManagedChannel channel,
        CallCredentials callCredentials,
        RateLimiter rateLimiter,
        List<ByteString> rowKeys) {
      stub = BigtableGrpc.newStub(channel).withCallCredentials(callCredentials);
      this.rateLimiter = rateLimiter;
      appProfileIds = resource.getAppProfileIds();
      this.rowKeys = rowKeys;

      instanceName = resource.extractInstanceName();
    }

    @Override
    public Void call() {
      while (true) {
        try {
          singleIteration();
        } catch (Exception e) {
          LOG.error("Failed to send RPC", e);
        }
      }
    }

    private void singleIteration()
        throws ExecutionException, InterruptedException, TimeoutException {
      rateLimiter.acquire();
      ByteString rowKey = rowKeys.get(random.nextInt(rowKeys.size()));
      String appProfileId = appProfileIds.get(random.nextInt(appProfileIds.size()));
      Stopwatch stopwatch = Stopwatch.createStarted();

      BigtableStub localStub = stub.withOption(APP_PROFILE_KEY, appProfileId);

      switch (loadType) {
        case PING:
          sendUnaryRpc(
              localStub::pingAndWarm,
              PingAndWarmRequest.newBuilder()
                  .setName(instanceName)
                  .setAppProfileId(appProfileId)
                  .build());
          break;
        case READ_ROW:
          sendReadRows(
              localStub,
              ReadRowsRequest.newBuilder()
                  .setTableName(resource.getTableName().toString())
                  .setAppProfileId(appProfileId)
                  .setRows(RowSet.newBuilder().addRowKeys(rowKey))
                  .setRowsLimit(1)
                  .build());
          break;
        case MUTATE_ROW:
          sendUnaryRpc(
              localStub::mutateRow,
              MutateRowRequest.newBuilder()
                  .setTableName(resource.getTableName().toString())
                  .setAppProfileId(appProfileId)
                  .setRowKey(rowKey)
                  .addMutations(
                      Mutation.newBuilder().setDeleteFromRow(DeleteFromRow.getDefaultInstance()))
                  .build());
          break;
        default:
          throw new IllegalStateException("Unknown load type: " + loadType);
      }

      long elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
      metrics.recordLatency(Duration.of(elapsed, ChronoUnit.MICROS), appProfileId);
    }

    private <ReqT, RespT> RespT sendUnaryRpc(BiConsumer<ReqT, StreamObserver<RespT>> fn, ReqT req)
        throws ExecutionException, InterruptedException, TimeoutException {
      CompletableFuture<RespT> f = new CompletableFuture<>();
      fn.accept(
          req,
          new StreamObserver<RespT>() {
            @Override
            public void onNext(RespT resp) {
              f.complete(resp);
            }

            @Override
            public void onError(Throwable throwable) {
              f.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
              f.complete(null);
            }
          });
      return f.get(30, TimeUnit.SECONDS);
    }

    private List<ReadRowsResponse> sendReadRows(BigtableStub stub, ReadRowsRequest request)
        throws ExecutionException, InterruptedException, TimeoutException {
      CompletableFuture<List<ReadRowsResponse>> f = new CompletableFuture<>();
      stub.withOption(APP_PROFILE_KEY, request.getAppProfileId())
          .readRows(
              request,
              new StreamObserver<ReadRowsResponse>() {
                List<ReadRowsResponse> results = new ArrayList<>();

                @Override
                public void onNext(ReadRowsResponse r) {
                  results.add(r);
                  int l = r.getChunksCount();
                  if (l > 0 && r.getChunksList().get(l - 1).getCommitRow()) {
                    f.complete(results);
                  }
                }

                @Override
                public void onError(Throwable throwable) {
                  f.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {
                  f.complete(results);
                }
              });
      return f.get(30, TimeUnit.SECONDS);
    }
  }

  private static final CallOptions.Key<String> APP_PROFILE_KEY = Key.create("APP_PROFILE_KEY");

  class MetadataInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      String appProfileId = callOptions.getOption(APP_PROFILE_KEY);

      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(call) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          Metadata metadata =
              Util.composeMetadata(
                  FeatureFlags.newBuilder()
                      .setDirectAccessRequested(target.getMode().enableDp)
                      .setTrafficDirectorEnabled(target.getMode().enableTd)
                      .build(),
                  ImmutableMap.of(
                      "table_name",
                      resource.getTableName().toString(),
                      "app_profile_id",
                      appProfileId));
          headers.merge(metadata);
          super.start(responseListener, headers);
        }
      };
    }
  }
}
