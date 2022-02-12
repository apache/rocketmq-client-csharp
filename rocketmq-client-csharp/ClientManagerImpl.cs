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

namespace org.apache.rocketmq {
    public class ClientManagerImpl : ClientManager {

        public ClientManagerImpl() {
            rpcClients = new ConcurrentDictionary<string, MessagingService.MessagingServiceClient>();
        }

        public MessagingService.MessagingServiceClient getRpcClient(string target) {
            if (!rpcClients.ContainsKey(target)) {
                using var channel = GrpcChannel.ForAddress(target);
                var client = new MessagingService.MessagingServiceClient(channel);
                if(rpcClients.TryAdd(target, client)) {
                    return client;
                }
            }
            return rpcClients[target];
        }

        private ConcurrentDictionary<string, MessagingService.MessagingServiceClient> rpcClients;

    }
}