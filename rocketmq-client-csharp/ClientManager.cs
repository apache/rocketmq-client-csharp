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


using rmq = apache.rocketmq.v1;
using System;
using System.Threading;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using System.Collections.Generic;

namespace org.apache.rocketmq {
    public class ClientManager : IClientManager {

        public ClientManager() {
            _rpcClients = new Dictionary<string, RpcClient>();
            _clientLock = new ReaderWriterLockSlim();
        }

        public IRpcClient getRpcClient(string target)
        {
            _clientLock.EnterReadLock();
            try
            {
                if (_rpcClients.ContainsKey(target))
                {
                    return _rpcClients[target];
                }
            }
            finally
            {
                _clientLock.ExitReadLock();
            }

            _clientLock.EnterWriteLock();
            try
            {
                if (_rpcClients.ContainsKey(target))
                {
                    return _rpcClients[target];
                }

                var client = new RpcClient(target);
                _rpcClients.Add(target, client);
                return client;
            }
            finally
            {
                _clientLock.ExitWriteLock();
            }
        }

        public async Task<TopicRouteData> resolveRoute(string target, grpc::Metadata metadata, rmq::QueryRouteRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var queryRouteResponse = await rpcClient.QueryRoute(metadata, request, timeout);

            if (queryRouteResponse.Common.Status.Code != ((int)Google.Rpc.Code.Ok))
            {
                // Raise an application layer exception
            }

            var partitions = new List<Partition>();
            // Translate protobuf object to domain specific one
            foreach (var partition in queryRouteResponse.Partitions)
            {
                var topic = new Topic(partition.Topic.ResourceNamespace, partition.Topic.Name);
                var id = partition.Id;
                Permission permission = Permission.READ_WRITE;
                switch (partition.Permission) {
                    case rmq::Permission.None:
                    {
                        permission = Permission.NONE;
                        break;
                    }
                    case rmq::Permission.Read:
                    {
                        permission = Permission.READ;
                        break;
                    }
                    case rmq::Permission.Write:
                    {
                        permission = Permission.WRITE;
                        break;
                    }
                    case rmq::Permission.ReadWrite:
                    {
                        permission = Permission.READ_WRITE;
                        break;
                    }
                }

                AddressScheme scheme = AddressScheme.IPv4;
                switch(partition.Broker.Endpoints.Scheme) {
                    case rmq::AddressScheme.Ipv4:
                        {
                        scheme = AddressScheme.IPv4;
                        break;
                    }
                    case rmq::AddressScheme.Ipv6:
                        {
                        scheme = AddressScheme.IPv6;
                        break;
                    }
                    case rmq::AddressScheme.DomainName:
                        {
                        scheme = AddressScheme.DOMAIN_NAME;
                        break;
                    }
                }

                List<Address> addresses = new List<Address>();
                foreach(var item in partition.Broker.Endpoints.Addresses) {
                    addresses.Add(new Address(item.Host, item.Port));
                }
                ServiceAddress serviceAddress = new ServiceAddress(scheme, addresses);
                Broker broker = new Broker(partition.Broker.Name, id, serviceAddress);
                partitions.Add(new Partition(topic, broker, id, permission));
            }

            var topicRouteData = new TopicRouteData(partitions);
            return topicRouteData;
        }

        public async Task<Boolean> heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var response = await rpcClient.Heartbeat(metadata, request, timeout);
            if (null == response)
            {
                return false;
            }

            return response.Common.Status.Code == (int)Google.Rpc.Code.Ok;
        }

        public async Task<rmq::SendMessageResponse> sendMessage(string target, grpc::Metadata metadata, rmq::SendMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var response = await rpcClient.SendMessage(metadata, request, timeout);
            return response;
        }

        public async Task<Boolean> notifyClientTermination(string target, grpc::Metadata metadata, rmq::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            rmq::NotifyClientTerminationResponse response = await rpcClient.NotifyClientTermination(metadata, request, timeout);
            return response.Common.Status.Code == ((int)Google.Rpc.Code.Ok);
        }

        private readonly Dictionary<string, RpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;
    }
}