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

package com.google.cloud.bigtable.jetstream.tools.core;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IpInterceptor implements ClientInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(IpInterceptor.class);
  private static final String DP_IPV6_PREFIX = "2001:4860:8040";
  private static final String DP_IPV4_PREFIX = "34.126";

  private final ConcurrentHashMap<SocketAddress, Boolean> seenSockets = new ConcurrentHashMap<>();

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    ClientCall<ReqT, RespT> clientCall = channel.newCall(methodDescriptor, callOptions);
    return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onHeaders(Metadata headers) {
                SocketAddress socketAddress =
                    clientCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                if (socketAddress != null && seenSockets.put(socketAddress, true) == null) {
                  LOG.info(
                      "Connected to new [{}] server: {}",
                      getSockLabel(socketAddress),
                      socketAddress);
                }
                super.onHeaders(headers);
              }
            },
            headers);
      }
    };
  }

  private static String getSockLabel(SocketAddress remoteAddress) {
    if (!(remoteAddress instanceof InetSocketAddress)) {
      return "unknown";
    }
    InetAddress inetAddress = ((InetSocketAddress) remoteAddress).getAddress();
    String addr = inetAddress.getHostAddress();

    if (addr.startsWith(DP_IPV6_PREFIX) || addr.startsWith(DP_IPV4_PREFIX)) {
      return "directpath";
    }

    return "cloudpath";
  }
}
