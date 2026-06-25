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

import com.google.protobuf.ByteString;
import site.ycsb.ByteIterator;

class ByteStringWrapper extends ByteIterator {

  private final ByteString.ByteIterator it;
  private int remaining;

  public ByteStringWrapper(ByteString value) {
    it = value.iterator();
    remaining = value.size();
  }

  @Override
  public boolean hasNext() {
    return it.hasNext();
  }

  @Override
  public byte nextByte() {
    remaining--;
    return it.nextByte();
  }

  @Override
  public long bytesLeft() {
    return remaining;
  }
}
