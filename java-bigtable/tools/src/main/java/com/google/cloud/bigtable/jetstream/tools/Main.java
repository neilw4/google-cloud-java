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

package com.google.cloud.bigtable.jetstream.tools;

import com.google.cloud.bigtable.jetstream.tools.commands.GenerateLoad;
import com.google.cloud.bigtable.jetstream.tools.commands.GenerateUnaryLoad;
import com.google.cloud.bigtable.jetstream.tools.commands.MutateRow;
import com.google.cloud.bigtable.jetstream.tools.commands.ReadRow;
import com.google.cloud.bigtable.jetstream.tools.commands.Ycsb;
import com.google.cloud.bigtable.jetstream.tools.commands.YcsbSummarize;
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "jetstream-tools",
    subcommands = {
      ReadRow.class,
      MutateRow.class,
      GenerateLoad.class,
      GenerateUnaryLoad.class,
      Ycsb.class,
      YcsbSummarize.class,
    },
    sortSynopsis = false,
    sortOptions = false,
    mixinStandardHelpOptions = true)
public class Main {
  @SuppressWarnings("InstantiationOfUtilityClass")
  public static void main(String[] args) {
    SLF4JBridgeHandler.install();

    new CommandLine(new Main())
        .setUsageHelpAutoWidth(true)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .execute(args);
  }
}
