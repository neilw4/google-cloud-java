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

import com.google.bigtable.v2.ClientConfiguration;
import com.google.bigtable.v2.FeatureFlags;
import com.google.bigtable.v2.PeerInfo;
import com.google.bigtable.v2.SessionResponse;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.ChannelProvider;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.CloudPath;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.ForwardingChannelProvider;
import com.google.cloud.bigtable.data.v2.internal.api.ChannelProviders.DirectAccess;
import com.google.cloud.bigtable.jetstream.tools.core.IpInterceptor;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;

/**
 * Mixin for specifying the target of the command. It supports 3 configurations types:
 *
 * <ul>
 *   <li>CloudPath - connect via a GFE
 *   <li>DirectPath - connect via a c2p DirectPath target
 *   <li>CloudPathTD - use traffic director, but request an endpoint via CFE
 *   <li>RawDirectPath - connect DirectPath by specifying an AFE ip/port
 * </ul>
 *
 * <p>In addition, the endpoint & port can be overridden for a mode using the {@code --endpoint}
 * option. The port is only relevant for DirectPath and CloudPath modes and defaults to 443.
 *
 * <p>By default the target will be DirectPath using the {@code
 * test-bigtable.sandbox.googleapis.com} endpoint.
 */
public class Target {
  private static final Logger LOG = LoggerFactory.getLogger(Target.class);

  public enum Mode {
    CloudPath(false, false),
    CloudPathTd(true, false),
    DirectPath(true, true),
    RawDirectPath(false, false),
    ;

    public final boolean enableTd;
    public final boolean enableDp;

    Mode(boolean enableTd, boolean enableDp) {
      this.enableTd = enableTd;
      this.enableDp = enableDp;
    }
  }

  @Option(
      names = "--mode",
      description = "Connection mode, valid options: ${COMPLETION-CANDIDATES}")
  Mode mode = Mode.DirectPath;

  @Option(
      names = "--endpoint",
      description =
          "The endpoint of the target.\n"
              + "  - For CloudPath, it can include both the hostname and port. \n"
              + "    ie \"test-bigtable.sandbox.googleapis.com:443\"\n"
              + "    Or just the host which will default the port to 443.\n"
              + "  - For CloudPathTd, it should be just be the c2p target without a port.\n"
              + "     ie \"test-bigtable.sandbox.googleapis.com\".\n"
              + "  - For RawDirectPath, it must be IP:port from the Borg summary page.\n"
              + "  - For DirectPath, it should be just be the c2p target without a port.\n"
              + "    ie \"test-bigtable.sandbox.googleapis.com\"\n",
      showDefaultValue = Visibility.ALWAYS)
  List<String> endpoints = Collections.singletonList(ChannelProviders.DEFAULT_HOST);

  @Option(names = "--dump-metadata", description = "Log metadata", showDefaultValue = Visibility.ALWAYS)
  boolean dumpMetadata = false;

  @Option(names = "--gfe-debug-headers", description = "Request GFE debug headers. Can be gfe_response_only, all_response or request_and_response", showDefaultValue = Visibility.ALWAYS, required = false)
  String requestGfeDebugHeaders = null;

  public Mode getMode() {
    return mode;
  }

  public List<String> getEndpoints() {
    return endpoints;
  }

  public ChannelProviders.ChannelProvider getChannelProvider() {
    ChannelProvider provider;
    switch (mode) {
      case CloudPath:
        Preconditions.checkArgument(
            getEndpoints().size() == 1, "Cloudpath only supports a single endpoint");
        provider = new CloudPath(getEndpoints().get(0));
        break;

      case RawDirectPath:
        Preconditions.checkArgument(
            !getEndpoints().isEmpty(), "Must specify at least one endpoint for raw directpath");
        provider = new ChannelProviders.RawDirectPath(getEndpoints());
        break;
      case DirectPath:
        Preconditions.checkArgument(endpoints.size() == 1, "DirectPath only supports one endpoint");
        provider = new DirectAccess(getEndpoints().get(0));
        break;
      default:
        throw new IllegalArgumentException("Unsupported target mode: " + mode);
    }
    LOG.info("Using ChannelProvider: {}", provider);

    return new ForwardingChannelProvider(provider) {
      @Override
      public ManagedChannelBuilder<?> newChannelBuilder(CallCredentials callCredentials) {
        ManagedChannelBuilder<?> builder = super.newChannelBuilder(callCredentials)
            .intercept(new IpInterceptor());
        if (dumpMetadata) {
          builder = builder.intercept(new MetadataInterceptor(requestGfeDebugHeaders));
        }
        return builder;
      }
    };
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("mode", mode)
        .add("endpoints", endpoints)
        .toString();
  }


  /**
   * Interceptor used to debug metadata between the client and the server
   */
  private static class MetadataInterceptor implements ClientInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataInterceptor.class);

    private static final Metadata.Key<String> REQUEST_PARAMS_KEY =
        Metadata.Key.of("x-goog-request-params", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> FEATURE_FLAGS_KEY =
        Metadata.Key.of("bigtable-features", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PEER_INFO_KEY =
        Key.of("bigtable-peer-info", Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> REQ_GFE_HEADERS = Key.of("X-Return-Encrypted-Headers", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> RESP_GFE_HEADERS = Key.of("X-Encrypted-Debug-Headers", Metadata.ASCII_STRING_MARSHALLER);
    private final String requestGfeDebugHeaders;

    public MetadataInterceptor(@Nullable String requestGfeDebugHeaders) {
      this.requestGfeDebugHeaders = requestGfeDebugHeaders;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

      return new SimpleForwardingClientCall<ReqT, RespT>(
          channel.newCall(methodDescriptor, callOptions)) {
        final AtomicInteger requestMsgIndex = new AtomicInteger(-1);


        @Override
        public void start(Listener<RespT> responseListener, Metadata requestHeaders) {
          if (requestGfeDebugHeaders != null) {
            requestHeaders.put(REQ_GFE_HEADERS, requestGfeDebugHeaders);
          }
          LOG.info("Stream {} Request Metadata: {}",
              methodDescriptor.getBareMethodName(),
              MoreObjects.toStringHelper(requestHeaders)
                  .add("featureFlags",
                      prettyPrintProto(extractProto(requestHeaders, FEATURE_FLAGS_KEY, FeatureFlags.parser())))
                  .add("x-google-params", requestHeaders.get(REQUEST_PARAMS_KEY))
                  .add("raw", requestHeaders));

          super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
            @Override
            public void onHeaders(Metadata responseHeaders) {
              LOG.info("Stream {} Response headers: {}",
                  methodDescriptor.getBareMethodName(),
                  MoreObjects.toStringHelper(responseHeaders)
                      .add("peerInfo",
                          prettyPrintProto(extractProto(responseHeaders, PEER_INFO_KEY, PeerInfo.parser())))
                      .add("gfeDebugHeaders", responseHeaders.get(RESP_GFE_HEADERS))
                      .add("raw", responseHeaders)
              );

              super.onHeaders(responseHeaders);
            }

            @Override
            public void onMessage(RespT message) {
              if (message instanceof SessionResponse) {
                SessionResponse sResp = (SessionResponse) message;
                switch (sResp.getPayloadCase()) {
                  case OPEN_SESSION:
                  case VIRTUAL_RPC:
                    LOG.info("Stream {} SessionResponse: {}", methodDescriptor.getBareMethodName(), prettyPrintProto(sResp));
                    break;
                }
              } else if (message instanceof ClientConfiguration) {
                LOG.info("ClientConfig: {}", message);
              } else {
                LOG.warn("Stream {} Unexpected response: {}", methodDescriptor.getBareMethodName(), prettyPrintProto(message));
              }
              super.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              LOG.info("Stream {} Response trailers: {}",
                  methodDescriptor.getBareMethodName(),
                  MoreObjects.toStringHelper(trailers)
                      .add("raw", trailers)
              );
              super.onClose(status, trailers);
            }
          }, requestHeaders);
        }

        @Override
        public void sendMessage(ReqT message) {
          if (requestMsgIndex.incrementAndGet() == 0) {
            LOG.info("Stream {} first msg: {}", methodDescriptor.getBareMethodName(), prettyPrintProto(message));
          }

          super.sendMessage(message);
        }
      };
    }
  }

  private static String prettyPrintProto(Object msg) {
    if (msg == null) {
      return "null";
    }
    if (msg instanceof Message) {
      try {
        return JsonFormat.printer().omittingInsignificantWhitespace().print((Message) msg);
      } catch (InvalidProtocolBufferException e) {
        return msg.toString();
      }
    }
    return msg.toString();
  }
  private static <T> T extractProto(Metadata md, Metadata.Key<String> key, com.google.protobuf.Parser<T> parser) {
    String encodedStr = md.get(key);
    byte[] decoded = Base64.getUrlDecoder().decode(encodedStr);
    try {
      return parser.parseFrom(decoded);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
