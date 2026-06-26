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
import com.google.bigtable.v2.FeatureFlags;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.data.v2.internal.api.Util;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Resource;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(
    name = "count-rows",
    description = "Count rows (uses classic api)",
    sortOptions = false,
    sortSynopsis = false,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class CountRows implements Callable<Void> {
  @Mixin private Target target;
  @Mixin private Resource resource;

  @Option(
      names = "--concurrency",
      description = "Number of parallel scanners, defaults to 4x the cpus",
      required = false)
  private Integer concurrency = null;

  @Option(
      names = "--channel-count",
      description = "Number of channels to use, defaults to 1/10 of the concurrency")
  private Integer channelCount = null;

  @Override
  public Void call() throws Exception {
    int effectiveConcurrency =
        Optional.ofNullable(this.concurrency)
            .orElseGet(() -> Runtime.getRuntime().availableProcessors() * 4);

    int effectiveChannelCount =
        Optional.ofNullable(this.channelCount)
            .orElseGet(() -> (int) Math.ceil(effectiveConcurrency / 10.0));

    System.out.println("Concurrency: " + effectiveConcurrency);
    System.out.println("Channels: " + effectiveChannelCount);

    Semaphore limiter = new Semaphore(effectiveConcurrency);

    CallCredentials callCredentials =
        MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
    List<ManagedChannel> channels = new ArrayList<>();

    for (int i = 0; i < effectiveChannelCount; i++) {
      channels.add(
          target
              .getChannelProvider()
              .newChannelBuilder()
              .intercept(new MetadataInterceptor())
              .build());
    }

    List<SampleRowKeysResponse> rowSamples =
        Lists.newArrayList(
            BigtableGrpc.newBlockingStub(channels.get(0))
                .withCallCredentials(callCredentials)
                .sampleRowKeys(
                    SampleRowKeysRequest.newBuilder()
                        .setAppProfileId(resource.getAppProfileId())
                        .setTableName(resource.getTableName().toString())
                        .build()));

    List<ReadRowsRequest> scanList = createScans(rowSamples);
    Collections.shuffle(scanList);
    BlockingDeque<ReadRowsRequest> scans = new LinkedBlockingDeque<>(scanList);
    CountDownLatch latch = new CountDownLatch(scans.size());

    AtomicLong totalCount = new AtomicLong();

    int i = 0;
    while (!scans.isEmpty()) {
      limiter.acquire();
      ReadRowsRequest req = scans.remove();
      Channel channel = channels.get((i++) % channels.size());

      BigtableGrpc.newStub(channel)
          .withCallCredentials(callCredentials)
          .readRows(
              req,
              new StreamObserver<ReadRowsResponse>() {
                long localCount = 0;

                @Override
                public void onNext(ReadRowsResponse value) {
                  for (CellChunk chunk : value.getChunksList()) {
                    if (chunk.getCommitRow()) {
                      localCount++;
                    }
                  }
                }

                @Override
                public void onError(Throwable t) {
                  System.err.println("got error: " + t);
                  System.exit(1);
                }

                @Override
                public void onCompleted() {
                  long l = totalCount.addAndGet(localCount);
                  System.out.println(l);
                  latch.countDown();
                  limiter.release();
                }
              });
    }
    latch.await();
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
    System.out.println("Total count: " + totalCount.get());

    return null;
  }

  List<ReadRowsRequest> createScans(List<SampleRowKeysResponse> rowSamples) {
    List<ReadRowsRequest> scans = new ArrayList<>();

    ReadRowsRequest templateReq =
        ReadRowsRequest.newBuilder()
            .setTableName(resource.getTableName().toString())
            .setAppProfileId(resource.getAppProfileId())
            .setFilter(
                RowFilter.newBuilder()
                    .setChain(
                        Chain.newBuilder()
                            .addFilters(RowFilter.newBuilder().setCellsPerRowLimitFilter(1))
                            .addFilters(RowFilter.newBuilder().setStripValueTransformer(true))))
            .build();

    ByteString lastKey = ByteString.EMPTY;

    for (SampleRowKeysResponse sample : rowSamples) {
      if (sample.getRowKey().isEmpty()) {
        continue;
      }

      scans.add(
          templateReq.toBuilder()
              .setRows(
                  RowSet.newBuilder()
                      .addRowRanges(
                          RowRange.newBuilder()
                              .setStartKeyOpen(lastKey)
                              .setEndKeyClosed(sample.getRowKey())))
              .build());
      lastKey = sample.getRowKey();
    }

    if (scans.isEmpty()) {
      scans.add(templateReq);
    } else if (!lastKey.isEmpty()) {
      scans.add(
          templateReq.toBuilder()
              .setRows(
                  RowSet.newBuilder().addRowRanges(RowRange.newBuilder().setStartKeyOpen(lastKey)))
              .build());
    }
    return scans;
  }

  class MetadataInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(call) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          Metadata metadata =
              Util.composeMetadata(
                  FeatureFlags.newBuilder()
                      .setDirectAccessRequested(false)
                      .setTrafficDirectorEnabled(false)
                      .build(),
                  ImmutableMap.of(
                      "table_name",
                      resource.getTableName().toString(),
                      "app_profile_id",
                      resource.getAppProfileId()));
          headers.merge(metadata);
          super.start(responseListener, headers);
        }
      };
    }
  }
}
