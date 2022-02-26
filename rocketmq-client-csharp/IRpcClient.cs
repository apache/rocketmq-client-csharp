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
using System.Threading.Tasks;
using Apache.Rocketmq.V1;
using Grpc.Core;

namespace Org.Apache.Rocketmq
{
    public interface IRpcClient
    {
        Task<QueryRouteResponse> QueryRoute(Metadata metadata, QueryRouteRequest request, TimeSpan timeout);

        Task<HeartbeatResponse> Heartbeat(Metadata metadata, HeartbeatRequest request, TimeSpan timeout);

        Task<HealthCheckResponse> HealthCheck(Metadata metadata, HealthCheckRequest request, TimeSpan timeout);

        Task<SendMessageResponse> SendMessage(Metadata metadata, SendMessageRequest request, TimeSpan timeout);

        Task<QueryAssignmentResponse> QueryAssignment(Metadata metadata, QueryAssignmentRequest request,
            TimeSpan timeout);

        Task<ReceiveMessageResponse> ReceiveMessage(Metadata metadata, ReceiveMessageRequest request, TimeSpan timeout);

        Task<AckMessageResponse> AckMessage(Metadata metadata, AckMessageRequest request, TimeSpan timeout);

        Task<NackMessageResponse> NackMessage(Metadata metadata, NackMessageRequest request, TimeSpan timeout);

        Task<ForwardMessageToDeadLetterQueueResponse> ForwardMessageToDeadLetterQueue(Metadata metadata,
            ForwardMessageToDeadLetterQueueRequest request, TimeSpan timeout);

        Task<EndTransactionResponse> EndTransaction(Metadata metadata, EndTransactionRequest request, TimeSpan timeout);

        Task<QueryOffsetResponse> QueryOffset(Metadata metadata, QueryOffsetRequest request, TimeSpan timeout);

        Task<PullMessageResponse> PullMessage(Metadata metadata, PullMessageRequest request, TimeSpan timeout);

        Task<PollCommandResponse> PollMessage(Metadata metadata, PollCommandRequest request, TimeSpan timeout);

        Task<ReportThreadStackTraceResponse> ReportThreadStackTrace(Metadata metadata,
            ReportThreadStackTraceRequest request, TimeSpan timeout);

        Task<ReportMessageConsumptionResultResponse> ReportMessageConsumptionResult(Metadata metadata,
            ReportMessageConsumptionResultRequest request, TimeSpan timeout);

        Task<NotifyClientTerminationResponse> NotifyClientTermination(Metadata metadata,
            NotifyClientTerminationRequest request, TimeSpan timeout);

        Task Shutdown();
    }
}