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

package com.google.cloud.bigtable.jetstream.tools.util;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.data.v2.internal.api.TableName;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.contrib.gcp.resource.GCPResourceProvider;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class Metrics {
  private final Attributes baseAttributes;
  private final Meter meter;
  private final LongHistogram latenciesUs;

  private static final List<Long> LATENCY_BUCKETS =
      ImmutableList.<Long>builder()
          // 0 - 10ms in 100 us steps
          .addAll(
              LongStream.iterate(0, d -> d + 100).limit(100).boxed().collect(Collectors.toList()))
          // [10 ms, 100 ms) in increments of 10ms
          .addAll(
              LongStream.iterate(10_000, d -> d + 10_000)
                  .limit(9)
                  .boxed()
                  .collect(Collectors.toList()))
          // [100ms, 10s) in increments of 100ms
          .addAll(
              LongStream.iterate(100_000, d -> d + 100_000)
                  .limit(9)
                  .boxed()
                  .collect(Collectors.toList()))
          .build();

  public static Metrics create(
      String type, Credentials credentials, Target target, TableName tableName) throws IOException {
    MetricConfiguration config =
        MetricConfiguration.builder()
            .setMetricServiceSettings(MetricServiceSettings.newBuilder().build())
            .setProjectId(tableName.getProjectId())
            .setCredentials(credentials)
            .setInstrumentationLibraryLabelsEnabled(false)
            .build();

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .setResource(Resource.create(new GCPResourceProvider().getAttributes()))
            .registerMetricReader(
                PeriodicMetricReader.builder(
                        GoogleCloudMetricExporter.createWithConfiguration(config))
                    .setInterval(Duration.ofMinutes(1))
                    .build())
            .registerMetricReader(
                PeriodicMetricReader.builder(LoggingMetricExporter.create())
                    .setInterval(Duration.ofMinutes(1))
                    .build())
            .build();

    Meter meter =
        meterProvider.meterBuilder("jetstream-tools").setInstrumentationVersion("0.0.1").build();
    Attributes attributes =
        Attributes.builder()
            .put("type", type)
            .put("mode", target.getMode().toString())
            .put("resource", tableName.toString())
            .build();
    return new Metrics(meter, attributes);
  }

  private Metrics(Meter meter, Attributes baseAttributes) {
    this.meter = meter;
    this.baseAttributes = baseAttributes;

    String prefix = "jetstream.tools.";

    latenciesUs =
        meter
            .histogramBuilder(prefix + "latency")
            .ofLongs()
            .setDescription("e2e latency")
            .setExplicitBucketBoundariesAdvice(LATENCY_BUCKETS)
            .setUnit("us")
            .build();
  }

  public void recordLatency(Duration latency, String appProfileId) {
    Attributes attributes = baseAttributes.toBuilder().put("app_profile", appProfileId).build();
    latenciesUs.record(TimeUnit.NANOSECONDS.toMicros(latency.toNanos()), attributes);
  }
}
