/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Net.Http;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using apache.rocketmq.v1;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace org.apache.rocketmq
{
    public class RpcClient : IRpcClient
    {
        private readonly MessagingService.MessagingServiceClient _stub;

        public RpcClient(string target)
        {
            var channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions
            {
                HttpHandler = CreateHttpHandler()
            });
            var invoker = channel.Intercept(new ClientLoggerInterceptor());
            _stub = new MessagingService.MessagingServiceClient(invoker);
        }

        /**
         * See https://docs.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-6.0 for performance consideration and
         * why parameters are configured this way.
         */
        private HttpMessageHandler CreateHttpHandler()
        {
            var sslOptions = new SslClientAuthenticationOptions();
            // Disable server certificate validation during development phase.
            // Comment out the following line if server certificate validation is required. 
            sslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
            var handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                EnableMultipleHttp2Connections = true,
                SslOptions = sslOptions,
            };
            return handler;
        }

        public async Task<QueryRouteResponse> QueryRoute(Metadata metadata, QueryRouteRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryRouteAsync(request, callOptions);
            return await call.ResponseAsync;
        }


        public async Task<HeartbeatResponse> Heartbeat(Metadata metadata, HeartbeatRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.HeartbeatAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<SendMessageResponse> SendMessage(Metadata metadata, SendMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.SendMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<NotifyClientTerminationResponse> NotifyClientTermination(Metadata metadata,
            NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.NotifyClientTerminationAsync(request, callOptions);
            return await call.ResponseAsync;
        }
    }
}