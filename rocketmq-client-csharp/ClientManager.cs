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

using System.Collections.Concurrent;

using apache.rocketmq.v1;
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;
using grpc = global::Grpc.Core;
using System.Collections.Generic;

namespace org.apache.rocketmq {
    public class ClientManager : IClientManager {

        public ClientManager() {
            rpcClients = new ConcurrentDictionary<string, RpcClient>();
        }

        public IRpcClient getRpcClient(string target) {
            if (!rpcClients.ContainsKey(target)) {
                using var channel = GrpcChannel.ForAddress(target);
                var client = new MessagingService.MessagingServiceClient(channel);
                var rpcClient = new RpcClient(client);
                if(rpcClients.TryAdd(target, rpcClient)) {
                    return rpcClient;
                }
            }
            return rpcClients[target];
        }

        public async Task<TopicRouteData> resolveRoute(string target, grpc::Metadata metadata, QueryRouteRequest request, TimeSpan timeout) {
            var rpcClient = getRpcClient(target);
            var callOptions = new grpc::CallOptions();
            callOptions.WithDeadline(DateTime.Now.Add(timeout));
            var queryRouteResponse = await rpcClient.queryRoute(request, callOptions);

            if (queryRouteResponse.Common.Status.Code != ((int)Google.Rpc.Code.Ok)) {
                // Raise an application layer exception

            }

            var partitions = new List<Partition>();
            // Translate protobuf object to domain specific one
            foreach (var partition in queryRouteResponse.Partitions) {
                

            } 

            var topicRouteData = new TopicRouteData(partitions);
            return topicRouteData;
        }

        private ConcurrentDictionary<string, RpcClient> rpcClients;

    }
}