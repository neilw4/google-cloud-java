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

import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Resource;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target.Mode;
import com.google.cloud.bigtable.jetstream.tools.ycsb.JetStreamDriver;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.db.GoogleBigtable2Client;
import site.ycsb.workloads.CoreWorkload;

@Command(
    name = "ycsb",
    description = "Run a YCSB benchmark",
    sortOptions = false,
    sortSynopsis = false,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class Ycsb implements Callable<Void> {
  enum Driver {
    Jetstream(JetStreamDriver.class),
    Classic(GoogleBigtable2Client.class);

    private final Class<? extends DB> cls;

    Driver(Class<? extends DB> cls) {
      this.cls = cls;
    }
  }

  @Mixin private Target target;
  @Mixin private Resource resource;

  @Option(names = "--driver", description = "The ycsb driver to use. Options: Jetstream, Classic")
  private Driver driver = Driver.Jetstream;

  @Option(names = "--threads", description = "Number of ycsb worker threads to use.")
  private int numThreads = 32;

  @Option(names = "--qps", description = "Maximum qps across all of the worker threads.")
  private int targetQps = 4_000;

  @Option(names = "--max-execution-time", description = "Duration of the benchmark.")
  private Duration maxExecutionTime = Duration.ofHours(1);

  @Option(
      names = "--operation-count",
      description = "Maximum operation count across all of the worker threads.")
  private int operationCount = 1_000_000_000;

  @Option(names = "--record-count", description = "How many records there are in the table.")
  private int recordCount = 1_111_111_111;

  @Option(names = "--field-count", description = "How many fields there are in each record.")
  private int fieldCount = 1;

  @Option(
      names = "--field-length",
      description = "How many bytes are in each field of each record.")
  private int fieldLength = 900;

  @Option(names = "--histogram-dir")
  private File histogramDir = new File("results");

  @Option(names = "--family", description = "Column family name")
  private String family = "cf";

  @Option(names = "--timestamp", description = "Timestamp of the writes")
  private long timestamp = System.currentTimeMillis() * 1000;

  @Option(names = "-p", description = "Additional ycsb parameters to passthrough", required = false)
  private Map<String, String> additionalParams = new HashMap<>();

  @Option(
      names = "--dataintegrity",
      description =
          "Enable data verification in Ycsb. This will add new counters to the YCSB. The counters will appear at the end of the run as `[VERIFY], Return=UNEXPECTED_STATE, $count`. Please note that this will only work on a table that was loaded with dataintegrity enabled.")
  private boolean dataIntegrity = false;

  @Option(names = "--load", description = "Read write portion")
  private Load load = Load.ALL_READ;

  enum Load {
    ALL_READ(1, 0),
    ALL_WRITE(0, 1),
    READ_WRITE(0.5, 0.5);

    private final double read;
    private final double write;

    Load(double read, double write) {
      this.read = read;
      this.write = write;
    }

    public double getRead() {
      return read;
    }

    public double getWrite() {
      return write;
    }
  }

  @Override
  public Void call() {
    if (!histogramDir.exists()) {
      System.out.println("Creating directory for histogram output");
      histogramDir.mkdirs();
    }
    Preconditions.checkState(histogramDir.isDirectory(), "failed to create histogram dir");
    File prefix =
        new File(histogramDir, new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-").format(new Date()));

    List<String> args = new ArrayList<>();

    args.addAll(
        Arrays.asList(
            "-db",
            driver.cls.getName(),

            // Load
            "-threads",
            Integer.toString(numThreads),
            "-p",
            String.format("target=%d", targetQps),
            "-p",
            String.format("operationcount=%d", operationCount),
            "-p",
            String.format("maxexecutiontime=%d", maxExecutionTime.getSeconds()),
            "-p",
            String.format("dataintegrity=%b", dataIntegrity),
            "-p",
            "readallfields=true",
            "-p",
            String.format("workload=%s", CoreWorkload.class.getName()),
            "-p",
            String.format("readproportion=%.2f", load.getRead()),
            "-p",
            "updateproportion=0",
            "-p",
            "scanproportion=0",
            "-p",
            String.format("insertproportion=%.2f", load.getWrite()),
            "-p",
            "requestdistribution=zipfian",

            // Data shape
            "-p",
            String.format("recordcount=%d", recordCount),
            "-p",
            String.format("fieldcount=%d", fieldCount),
            "-p",
            String.format("fieldlength=%d", fieldLength),

            // Status reporting
            "-s",
            "-p",
            "status.interval=10",
            "-p",
            "measurementtype=hdrhistogram",
            "-p",
            "measurement.interval=op",
            "-p",
            "reportlatencyforeacherror=true",

            // histogram
            "-p",
            "hdrhistogram.percentiles=50,99",
            "-p",
            "hdrhistogram.fileoutput=true",
            "-p",
            String.format("hdrhistogram.output.path=%s", prefix)));

    switch (driver) {
      case Jetstream:
        args.addAll(
            Arrays.asList(
                // Bigtable specific
                "-p",
                String.format("%s=%s", JetStreamDriver.TRANSPORT_KEY, target.getMode().name()),
                "-p",
                String.format(
                    "%s=%s",
                    JetStreamDriver.ENDPOINTS_KEY, Joiner.on(",").join(target.getEndpoints())),
                "-p",
                String.format(
                    "%s=%s",
                    JetStreamDriver.INSTANCE_NAME_KEY, resource.getTableName().getInstanceName()),
                "-p",
                String.format("%s=%s", JetStreamDriver.APP_PROFILE_KEY, resource.getAppProfileId()),
                "-p",
                String.format("table=%s", resource.getTableName().getTableId()),
                "-p",
                String.format("%s=%s", JetStreamDriver.COLUMN_FAMILY_KEY, family),
                "-p",
                String.format("%s=%s", JetStreamDriver.TIMESTAMP_KEY, timestamp)));
        break;
      case Classic:
        Preconditions.checkArgument(
            target.getMode() == Mode.DirectPath, "Classic mode is hardcoded to use DirectPath");
        Preconditions.checkArgument(
            Collections.singletonList(ChannelProviders.DEFAULT_HOST).equals(target.getEndpoints()),
            "Classic mode is hardcoded to use " + ChannelProviders.DEFAULT_HOST);
        args.addAll(
            Arrays.asList(
                "-p",
                "googlebigtable2.data-endpoint=" + target.getEndpoints().get(0),
                "-p",
                "googlebigtable2.project=" + resource.getTableName().getProjectId(),
                "-p",
                "googlebigtable2.instance=" + resource.getTableName().getInstanceId(),
                "-p",
                "table=" + resource.getTableName().getTableId(),
                "-p",
                "googlebigtable2.app-profile=" + resource.getAppProfileId(),
                "-p",
                "googlebigtable2.family=" + family,
                "-p",
                "googlebigtable2.timestamp=" + timestamp));
        break;
      default:
        throw new IllegalArgumentException("Unknown driver: " + driver);
    }

    // Inject arbitrary ycsb proprties
    for (Entry<String, String> e : additionalParams.entrySet()) {
      args.add("-p");
      args.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }

    Client.main(args.toArray(new String[0]));

    return null;
  }
}
