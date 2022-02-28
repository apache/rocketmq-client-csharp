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
using Apache.Rocketmq.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace Org.Apache.Rocketmq
{
    public class RpcClient : IRpcClient
    {
        private readonly MessagingService.MessagingServiceClient _stub;
        private readonly GrpcChannel _channel;

        public RpcClient(string target)
        {
            _channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions
            {
                HttpHandler = CreateHttpHandler()
            });
            var invoker = _channel.Intercept(new ClientLoggerInterceptor());
            _stub = new MessagingService.MessagingServiceClient(invoker);
        }

        public async Task Shutdown()
        {
            if (null != _channel)
            {
                await _channel.ShutdownAsync();
            }
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

        public async Task<HealthCheckResponse> HealthCheck(Metadata metadata, HealthCheckRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.HealthCheckAsync(request, callOptions);
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

        public async Task<QueryAssignmentResponse> QueryAssignment(Metadata metadata, QueryAssignmentRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryAssignmentAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<ReceiveMessageResponse> ReceiveMessage(Metadata metadata, ReceiveMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ReceiveMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<AckMessageResponse> AckMessage(Metadata metadata, AckMessageRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.AckMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<NackMessageResponse> NackMessage(Metadata metadata, NackMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.NackMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<ForwardMessageToDeadLetterQueueResponse> ForwardMessageToDeadLetterQueue(Metadata metadata,
            ForwardMessageToDeadLetterQueueRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ForwardMessageToDeadLetterQueueAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<EndTransactionResponse> EndTransaction(Metadata metadata, EndTransactionRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.EndTransactionAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<QueryOffsetResponse> QueryOffset(Metadata metadata, QueryOffsetRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryOffsetAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<PullMessageResponse> PullMessage(Metadata metadata, PullMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.PullMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<PollCommandResponse> PollMessage(Metadata metadata, PollCommandRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.PollCommandAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<ReportThreadStackTraceResponse> ReportThreadStackTrace(Metadata metadata,
            ReportThreadStackTraceRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ReportThreadStackTraceAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<ReportMessageConsumptionResultResponse> ReportMessageConsumptionResult(Metadata metadata,
            ReportMessageConsumptionResultRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ReportMessageConsumptionResultAsync(request, callOptions);
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