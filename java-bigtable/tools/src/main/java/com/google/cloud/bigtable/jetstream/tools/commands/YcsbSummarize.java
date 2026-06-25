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

import com.google.cloud.bigtable.jetstream.tools.commands.YcsbSummarize.MyDefaultValueProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.io.PatternFilenameFilter;
import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "ycsb-summarize",
    description = "Summarize a timed histogram result from a previous run of a ycsb benchmark.",
    sortOptions = false,
    sortSynopsis = false,
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
    defaultValueProvider = MyDefaultValueProvider.class)
public class YcsbSummarize implements Callable<Void> {
  @Parameters(
      description =
          "Path to the ycsb histogram file. By default uses the latest READ hdr file in results/")
  private File file;

  @Option(
      names = "--skip",
      description = "Amount of time to exclude from the beginning of the histogram")
  private Duration skip = Duration.ZERO;

  @Override
  public Void call() throws Exception {
    Histogram overallHistogram = new Histogram(3);

    Instant reportStartAt = null;

    try (FileInputStream fin = new FileInputStream(file)) {
      HistogramLogReader reader = new HistogramLogReader(fin);
      EncodableHistogram encodeableHistogram;

      while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
        Histogram histogram = (Histogram) encodeableHistogram;

        Instant histogramTimestamp = Instant.ofEpochMilli(histogram.getStartTimeStamp());
        if (reportStartAt == null) {
          reportStartAt = histogramTimestamp.plus(skip);
        }

        if (!histogramTimestamp.isBefore(reportStartAt)) {
          overallHistogram.add(histogram);
        }
      }
    }
    Duration duration =
        Duration.ofMillis(
            overallHistogram.getEndTimeStamp() - overallHistogram.getStartTimeStamp());
    System.out.printf("Duration: %s%n", duration);
    System.out.printf("Rpc Count: %,d%n", overallHistogram.getTotalCount());
    System.out.printf(
        "Throughput: %,d qps%n", overallHistogram.getTotalCount() / duration.getSeconds());

    for (Integer percentile : ImmutableList.of(50, 99)) {
      long valueUs = overallHistogram.getValueAtPercentile(percentile);

      System.out.printf("p%d: %,d us%n", percentile, valueUs);
    }

    return null;
  }

  static class MyDefaultValueProvider implements IDefaultValueProvider {

    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {

      if (argSpec.isPositional()) {
        return Optional.ofNullable(
                new File("results").listFiles(new PatternFilenameFilter(".*(READ)\\.hdr")))
            .map(Arrays::stream)
            .orElse(Stream.empty())
            .sorted(Comparator.reverseOrder())
            .map(File::toString)
            .findFirst()
            .orElse(null);
      }
      return null;
    }
  }
}
