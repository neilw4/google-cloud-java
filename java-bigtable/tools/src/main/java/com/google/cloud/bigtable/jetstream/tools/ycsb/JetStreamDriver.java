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

package com.google.cloud.bigtable.jetstream.tools.ycsb;

import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.OpenTableRequest.Permission;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.SessionMutateRowRequest;
import com.google.bigtable.v2.SessionMutateRowResponse;
import com.google.bigtable.v2.SessionReadRowRequest;
import com.google.bigtable.v2.SessionReadRowResponse;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.CloudPath;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.RawDirectPath;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.DirectAccess;
import com.google.cloud.bigtable.data.v2.internal.api.Client;
import com.google.cloud.bigtable.data.v2.internal.api.ClientSettings;
import com.google.cloud.bigtable.data.v2.internal.api.InstanceName;
import com.google.cloud.bigtable.data.v2.internal.api.TableAsync;
import com.google.cloud.bigtable.jetstream.tools.commands.args.Target.Mode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

public class JetStreamDriver extends site.ycsb.DB {
  static final String PROP_PREFIX = "bigtable";
  public static final String TRANSPORT_KEY = PROP_PREFIX + ".transport";
  public static final String ENDPOINTS_KEY = PROP_PREFIX + ".endpoints";
  public static final String INSTANCE_NAME_KEY = PROP_PREFIX + ".instance-name";
  public static final String APP_PROFILE_KEY = PROP_PREFIX + ".app-profile-id";

  public static final String COLUMN_FAMILY_KEY = PROP_PREFIX + ".family";
  public static final String TIMESTAMP_KEY = PROP_PREFIX + ".timestamp";

  private static final Logger log = LoggerFactory.getLogger(JetStreamDriver.class);

  private static final Object lock = new Object();
  private static int refCount = 0;

  private static Client client = null;
  private static TableAsync readTable = null;
  private static TableAsync writeTable = null;
  private static String tableId = null;

  private static String family = null;
  private static long timestamp = 0;

  @Override
  public void init() throws DBException {
    synchronized (lock) {
      refCount++;
      if (refCount > 1) {
        return;
      }

      ClientSettings.Builder builder =
          ClientSettings.builder()
              .setInstanceName(InstanceName.parse(getRequiredProp(INSTANCE_NAME_KEY)))
              .setAppProfileId(getRequiredProp(APP_PROFILE_KEY));

      List<String> endpoints = ImmutableList.copyOf(getRequiredProp(ENDPOINTS_KEY).split(","));

      Mode mode = Mode.valueOf(getRequiredProp(TRANSPORT_KEY));
      switch (mode) {
        case CloudPath:
          Preconditions.checkArgument(
              endpoints.size() == 1, "CloudPath must have exactly 1 endpoint");
          builder.setChannelProvider(new CloudPath(endpoints.get(0)));
          break;
        // case CloudPathTd:
        //   Preconditions.checkArgument(
        //       endpoints.size() == 1, "CloudPathTd must have exactly 1 endpoint");
        //   builder.setChannelProvider(new TrafficDirector(endpoints.get(0), false));
        //   break;
        case DirectPath:
          Preconditions.checkArgument(
              endpoints.size() == 1, "DirectPath must have exactly 1 endpoint");
          builder.setChannelProvider(new DirectAccess(endpoints.get(0)));
          break;
        case RawDirectPath:
          {
            Preconditions.checkArgument(!endpoints.isEmpty(), "Must specify at least one endpoint");
            builder.setChannelProvider(new RawDirectPath(endpoints));
            break;
          }
        default:
          throw new IllegalArgumentException("Unknown mode: " + mode);
      }

      try {
        client = new Client(builder.build());
      } catch (Exception e) {
        throw new DBException("Failed to initialize client", e);
      }

      tableId = getRequiredProp("table");

      family = getRequiredProp(COLUMN_FAMILY_KEY);

      timestamp = Long.getLong(TIMESTAMP_KEY, System.currentTimeMillis() * 1000);

      // TODO: merge this after READ_WRITE permission is supported
      readTable = client.openTableAsync(tableId, Permission.PERMISSION_READ);
      writeTable = client.openTableAsync(tableId, Permission.PERMISSION_WRITE);

      try {
        readTable
            .readRow(
                SessionReadRowRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("non-existent-key"))
                    .setFilter(RowFilter.newBuilder().setBlockAllFilter(true).build())
                    .build(),
                Deadline.after(1, TimeUnit.MINUTES))
            .get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DBException("Interrupted while priming session", e);
      } catch (ExecutionException e) {
        throw new DBException("Failed to prime session", e.getCause());
      }
    }
  }

  private String getRequiredProp(String key) {
    String value = getProperties().getProperty(key);
    Preconditions.checkState(!Strings.isNullOrEmpty(value), "Missing property: %s", key);
    return value;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (lock) {
      refCount--;
      if (refCount > 0) {
        return;
      }
    }

    readTable.close();
    writeTable.close();

    try {
      client.close();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Preconditions.checkArgument(tableId.equals(table), "table must not change");

    CompletableFuture<SessionReadRowResponse> f =
        readTable.readRow(
            SessionReadRowRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setFilter(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build())
                .build(),
            Deadline.after(1, TimeUnit.MINUTES));

    Row row;
    try {
      row = f.get().getRow();
    } catch (ExecutionException e) {
      log.error("read failed", e);
      return Status.ERROR;
    } catch (InterruptedException e) {
      log.error("read interrupted", e);
      Thread.currentThread().interrupt();
      return Status.ERROR;
    }

    for (Family family : row.getFamiliesList()) {
      for (Column column : family.getColumnsList()) {
        result.put(
            column.getQualifier().toStringUtf8(),
            new ByteStringWrapper(column.getCells(0).getValue()));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return set(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return set(table, key, values);
  }

  private Status set(String table, String key, Map<String, ByteIterator> values) {
    Preconditions.checkState(tableId.equals(table), "table must not change");

    SessionMutateRowRequest.Builder request =
        SessionMutateRowRequest.newBuilder().setKey(ByteString.copyFromUtf8(key));

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      Mutation mutation =
          Mutation.newBuilder()
              .setSetCell(
                  Mutation.SetCell.newBuilder()
                      .setFamilyName(family)
                      .setColumnQualifier(ByteString.copyFromUtf8(entry.getKey()))
                      .setValue(ByteString.copyFrom(entry.getValue().toArray()))
                      .setTimestampMicros(timestamp)
                      .build())
              .build();

      request.addMutations(mutation);
    }

    CompletableFuture<SessionMutateRowResponse> f =
        writeTable.mutateRow(request.build(), Deadline.after(1, TimeUnit.MINUTES));

    try {
      f.get();
    } catch (ExecutionException e) {
      log.error("mutate failed", e);
      return Status.ERROR;
    } catch (InterruptedException e) {
      log.error("mutate failed", e);
      Thread.currentThread().interrupt();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    Preconditions.checkState(tableId.equals(table), "table must not change");

    SessionMutateRowRequest request =
        SessionMutateRowRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .addMutations(
                Mutation.newBuilder().setDeleteFromRow(Mutation.DeleteFromRow.getDefaultInstance()))
            .build();

    CompletableFuture<SessionMutateRowResponse> f =
        writeTable.mutateRow(request, Deadline.after(1, TimeUnit.MINUTES));

    try {
      f.get();
    } catch (ExecutionException e) {
      log.error("mutate failed", e);
      return Status.ERROR;
    } catch (InterruptedException e) {
      log.error("mutate failed", e);
      Thread.currentThread().interrupt();
      return Status.ERROR;
    }
    return Status.OK;
  }
}
