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

package com.google.cloud.bigtable.jetstream.tools.commands.args;

import com.google.cloud.bigtable.data.v2.internal.api.TableName;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;

public class Resource {
  @Option(
      names = "--table-name",
      description = "The table name to use when opening a session",
      showDefaultValue = Visibility.ALWAYS,
      converter = TableNameConverter.class)
  private TableName tableName =
      TableName.parse(
          "projects/google.com:cloud-bigtable-dev/instances/igor-ew1c/tables/workloadc");

  @Option(
      names = "--app-profile-id",
      description = "The app profile id to use when opening a session",
      showDefaultValue = Visibility.ALWAYS)
  private String appProfileId = "default";

  public TableName getTableName() {
    return tableName;
  }

  public String getAppProfileId() {
    return appProfileId;
  }

  private static final class TableNameConverter implements ITypeConverter<TableName> {
    @Override
    public TableName convert(String value) {
      return TableName.parse(value);
    }
  }

  @Override
  public String toString() {
    return "Resource{" + "tableName=" + tableName + ", appProfileId='" + appProfileId + '\'' + '}';
  }
}
