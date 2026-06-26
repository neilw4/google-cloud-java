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

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.OpenTableRequest;
import com.google.bigtable.v2.SessionMutateRowRequest;
import com.google.bigtable.v2.SessionMutateRowResponse;
import com.google.cloud.bigtable.data.v2.internal.api.Client;
import com.google.cloud.bigtable.data.v2.internal.api.ClientSettings;
import com.google.cloud.bigtable.data.v2.internal.api.TableAsync;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Resource;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/*
Example:

run.sh mutaterow \
  --mode directpath \
  --endpoint test-bigtable.sandbox.googleapis.com \
  --app-profile-id default \
  --table-name projects/google.com:cloud-bigtable-dev/instances/clusterpin-bidi-tm/tables/ycsb \
  --key some-row --values cf:q=value,cf:q1=value1
 */
@Command(
    name = "mutaterow",
    description = "Mutate a row",
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class MutateRow implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRow.class);

  @Mixin private Target target;
  @Mixin private Resource resource;

  @Option(names = "--key", description = "row key as a string", required = true)
  private String rowKey;

  @Option(
      names = "--values",
      description = "cell values formated as family:col=value",
      required = true)
  private Map<String, String> columnValues;

  @Override
  public Void call() throws Exception {
    ClientSettings clientSettings =
        ClientSettings.builder()
            .setChannelProvider(target.getChannelProvider())
            .setInstanceName(resource.getTableName().getInstanceName())
            .setAppProfileId(resource.getAppProfileId())
            .build();

    try (Client client = Client.create(clientSettings);
        TableAsync table =
            client.openTableAsync(
                resource.getTableName().getTableId(),
                OpenTableRequest.Permission.PERMISSION_WRITE)) {
      long timestamp = System.currentTimeMillis();
      SessionMutateRowRequest.Builder request =
          SessionMutateRowRequest.newBuilder().setKey(ByteString.copyFromUtf8(rowKey));

      for (Entry<String, String> entry : columnValues.entrySet()) {
        String[] parts = entry.getKey().split(":", 2);
        String family = parts[0];
        String col = parts[1];

        request.addMutations(
            Mutation.newBuilder()
                .setSetCell(
                    SetCell.newBuilder()
                        .setFamilyName(family)
                        .setColumnQualifier(ByteString.copyFromUtf8(col))
                        .setValue(ByteString.copyFromUtf8(entry.getValue()))));
      }
      CompletionStage<SessionMutateRowResponse> f =
          table.mutateRow(request.build(), Deadline.after(1, TimeUnit.MINUTES));

      LOG.info("MUTATE_ROW OK: {}", f.toCompletableFuture().get());
    }

    return null;
  }
}
