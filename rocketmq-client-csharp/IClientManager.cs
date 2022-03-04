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

using Apache.Rocketmq.V1;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using grpc = global::Grpc.Core;
using rmq = Apache.Rocketmq.V1;


namespace Org.Apache.Rocketmq {
    public interface IClientManager {
        IRpcClient GetRpcClient(string target);

        Task<TopicRouteData> ResolveRoute(string target, grpc::Metadata metadata, QueryRouteRequest request, TimeSpan timeout);

        Task<Boolean> Heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request, TimeSpan timeout);

        Task<Boolean> HealthCheck(string target, grpc::Metadata metadata, rmq::HealthCheckRequest request, TimeSpan timeout);

        Task<Boolean> NotifyClientTermination(string target, grpc::Metadata metadata, NotifyClientTerminationRequest request, TimeSpan timeout);

        Task<SendMessageResponse> SendMessage(string target, grpc::Metadata metadata, SendMessageRequest request, TimeSpan timeout);

        Task<List<Assignment>> QueryLoadAssignment(string target, grpc::Metadata metadata, QueryAssignmentRequest request, TimeSpan timeout);

        Task<List<Message>> ReceiveMessage(string target, grpc::Metadata metadata, ReceiveMessageRequest request, TimeSpan timeout);


        Task<Boolean> Ack(string target, grpc::Metadata metadata, AckMessageRequest request, TimeSpan timeout);

        Task<Boolean> Nack(string target, grpc::Metadata metadata, NackMessageRequest request, TimeSpan timeout);

        Task Shutdown();
    }
}