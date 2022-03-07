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

using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System;
using rmq = Apache.Rocketmq.V1;
using grpc = global::Grpc.Core;
using NLog;


namespace Org.Apache.Rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {
        protected static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public Client(INameServerResolver resolver, string resourceNamespace)
        {
            _nameServerResolver = resolver;
            _resourceNamespace = resourceNamespace;
            Manager = ClientManagerFactory.getClientManager(resourceNamespace);
            _nameServerResolverCts = new CancellationTokenSource();

            _topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            _updateTopicRouteCts = new CancellationTokenSource();

            _healthCheckCts = new CancellationTokenSource();
        }

        public virtual void Start()
        {
            schedule(async () =>
            {
                await UpdateNameServerList();
            }, 30, _nameServerResolverCts.Token);

            schedule(async () =>
            {
                await UpdateTopicRoute();

            }, 30, _updateTopicRouteCts.Token);

            schedule(async () =>
            {
                await HealthCheck();
            }, 30, _healthCheckCts.Token);

        }

        public virtual void Shutdown()
        {
            Logger.Info($"Shutdown client[resource-namespace={_resourceNamespace}");
            _updateTopicRouteCts.Cancel();
            _nameServerResolverCts.Cancel();
            Manager.Shutdown().GetAwaiter().GetResult();
        }

        protected string FilterBroker(Func<string, bool> acceptor)
        {
            foreach (var item in _topicRouteTable)
            {
                foreach (var partition in item.Value.Partitions)
                {
                    string target = partition.Broker.TargetUrl();
                    if (acceptor(target))
                    {
                        return target;
                    }
                }
            }
            return null;
        }

        /**
         * Return all endpoints of brokers in route table.
         */
        private List<string> AvailableBrokerEndpoints()
        {
            List<string> endpoints = new List<string>();
            foreach (var item in _topicRouteTable)
            {
                foreach (var partition in item.Value.Partitions)
                {
                    string endpoint = partition.Broker.TargetUrl();
                    if (!endpoints.Contains(endpoint))
                    {
                        endpoints.Add(endpoint);
                    }
                }
            }
            return endpoints;
        }

        private async Task UpdateNameServerList()
        {
            List<string> nameServers = await _nameServerResolver.resolveAsync();
            if (0 == nameServers.Count)
            {
                // Whoops, something should be wrong. We got an empty name server list.
                Logger.Warn("Got an empty name server list");
                return;
            }

            if (nameServers.Equals(this._nameServers))
            {
                Logger.Debug("Name server list remains unchanged");
                return;
            }

            // Name server list is updated. 
            // TODO: Locking is required
            this._nameServers = nameServers;
            this._currentNameServerIndex = 0;
        }

        private async Task UpdateTopicRoute()
        {
            if (null == _nameServers || 0 == _nameServers.Count)
            {
                List<string> list = await _nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this._nameServers = list;
                }
                else
                {
                    Logger.Error("Failed to resolve name server list");
                    return;
                }
            }

            // We got one or more name servers available.
            string nameServer = _nameServers[_currentNameServerIndex];

            List<Task<TopicRouteData>> tasks = new List<Task<TopicRouteData>>();
            foreach (var item in _topicRouteTable)
            {
                tasks.Add(GetRouteFor(item.Key, true));
            }

            // Update topic route data
            TopicRouteData[] result = await Task.WhenAll(tasks);
            foreach (var item in result)
            {
                if (null == item)
                {
                    continue;
                }

                if (0 == item.Partitions.Count)
                {
                    continue;
                }

                var topicName = item.Partitions[0].Topic.Name;
                var existing = _topicRouteTable[topicName];
                if (!existing.Equals(item))
                {
                    _topicRouteTable[topicName] = item;
                }
            }
        }

        public void schedule(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                // TODO: log warning
                return;
            }

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    action();
                    await Task.Delay(TimeSpan.FromSeconds(seconds), token);
                }
            });
        }

        protected rmq::Endpoints AccessEndpoint(string nameServer)
        {
            var endpoints = new rmq::Endpoints();
            endpoints.Scheme = global::Apache.Rocketmq.V1.AddressScheme.Ipv4;
            var address = new global::Apache.Rocketmq.V1.Address();
            int pos = nameServer.LastIndexOf(':');
            address.Host = nameServer.Substring(0, pos);
            address.Port = Int32.Parse(nameServer.Substring(pos + 1));
            endpoints.Addresses.Add(address);
            return endpoints;
        }

        /**
         * Parameters:
         * topic
         *    Topic to query
         * direct
         *    Indicate if we should by-pass cache and fetch route entries from name server.
         */
        public async Task<TopicRouteData> GetRouteFor(string topic, bool direct)
        {
            if (!direct && _topicRouteTable.ContainsKey(topic))
            {
                return _topicRouteTable[topic];
            }

            if (null == _nameServers || 0 == _nameServers.Count)
            {
                List<string> list = await _nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this._nameServers = list;
                }
                else
                {
                    Logger.Error("Name server is not properly configured. List is null or empty");
                    return null;
                }
            }


            for (int retry = 0; retry < MaxTransparentRetry; retry++)
            {
                // We got one or more name servers available.
                int index = (_currentNameServerIndex + retry) % _nameServers.Count;
                string nameServer = _nameServers[index];
                var request = new rmq::QueryRouteRequest();
                request.Topic = new rmq::Resource();
                request.Topic.ResourceNamespace = _resourceNamespace;
                request.Topic.Name = topic;
                request.Endpoints = AccessEndpoint(nameServer);
                var metadata = new grpc.Metadata();
                Signature.sign(this, metadata);
                string target = $"https://{nameServer}";
                TopicRouteData topicRouteData;
                try
                {
                    topicRouteData = await Manager.ResolveRoute(target, metadata, request, getIoTimeout());
                    if (null != topicRouteData)
                    {
                        Logger.Debug($"Got route entries for {topic} from name server");
                        _topicRouteTable.TryAdd(topic, topicRouteData);

                        if (retry > 0)
                        {
                            _currentNameServerIndex = index;
                        }
                        return topicRouteData;
                    }
                    else
                    {
                        Logger.Warn($"Failed to query route of {topic} from {target}");
                    }
                }
                catch (System.Exception e)
                {
                    Logger.Warn(e, "Failed when querying route");
                }
            }
            return null;
        }

        public abstract void PrepareHeartbeatData(rmq::HeartbeatRequest request);

        public async Task Heartbeat()
        {
            List<string> endpoints = AvailableBrokerEndpoints();
            if (0 == endpoints.Count)
            {
                Logger.Debug("No broker endpoints available in topic route");
                return;
            }

            var request = new rmq::HeartbeatRequest();
            PrepareHeartbeatData(request);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);

            List<Task> tasks = new List<Task>();
            foreach (var endpoint in endpoints)
            {
                tasks.Add(Manager.Heartbeat(endpoint, metadata, request, getIoTimeout()));
            }

            await Task.WhenAll(tasks);
        }

        private List<string> BlockedBrokerEndpoints()
        {
            List<string> endpoints = new List<string>();
            return endpoints;
        }

        public async Task HealthCheck()
        {
            var request = new rmq::HealthCheckRequest();

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            List<Task<Boolean>> tasks = new List<Task<Boolean>>();
            List<string> endpoints = BlockedBrokerEndpoints();
            foreach (var endpoint in endpoints)
            {
                tasks.Add(Manager.HealthCheck(endpoint, metadata, request, getIoTimeout()));
            }
            var result = await Task.WhenAll(tasks);
            int i = 0;
            foreach (var ok in result)
            {
                if (ok)
                {
                    RemoveFromBlockList(endpoints[i]);
                }
                ++i;
            }
        }

        private void RemoveFromBlockList(string endpoint)
        {

        }

        public async Task<List<rmq::Assignment>> scanLoadAssignment(string topic, string group)
        {
            // Pick a broker randomly
            string target = FilterBroker((s) => true);
            var request = new rmq::QueryAssignmentRequest();
            request.ClientId = clientId();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;
            request.Endpoints = AccessEndpoint(_nameServers[_currentNameServerIndex]);
            try
            {
                var metadata = new grpc::Metadata();
                Signature.sign(this, metadata);
                return await Manager.QueryLoadAssignment(target, metadata, request, getIoTimeout());
            }
            catch (System.Exception e)
            {
                Logger.Warn(e, $"Failed to acquire load assignments from {target}");
            }
            // Just return an empty list.
            return new List<rmq.Assignment>();
        }

        private string TargetUrl(rmq::Assignment assignment)
        {
            var broker = assignment.Partition.Broker;
            var addresses = broker.Endpoints.Addresses;
            // TODO: use the first address for now. 
            var address = addresses[0];
            return $"https://{address.Host}:{address.Port}";
        }


        public async Task<List<Message>> ReceiveMessage(rmq::Assignment assignment, string group)
        {
            var targetUrl = TargetUrl(assignment);
            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            var request = new rmq::ReceiveMessageRequest();
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;
            request.Partition = assignment.Partition;
            var messages = await Manager.ReceiveMessage(targetUrl, metadata, request, getLongPollingTimeout());
            return messages;
        }

        public async Task<Boolean> Ack(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::AckMessageRequest();
            request.ClientId = clientId();
            request.ReceiptHandle = receiptHandle;
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;

            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;

            request.MessageId = messageId;

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            return await Manager.Ack(target, metadata, request, getIoTimeout());
        }

        public async Task<Boolean> Nack(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::NackMessageRequest();
            request.ClientId = clientId();
            request.ReceiptHandle = receiptHandle;
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;

            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;

            request.MessageId = messageId;

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            return await Manager.Nack(target, metadata, request, getIoTimeout());
        }

        public async Task<bool> NotifyClientTermination()
        {
            List<string> endpoints = AvailableBrokerEndpoints();
            var request = new rmq::NotifyClientTerminationRequest();
            request.ClientId = clientId();

            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);

            List<Task<Boolean>> tasks = new List<Task<Boolean>>();

            foreach (var endpoint in endpoints)
            {
                tasks.Add(Manager.NotifyClientTermination(endpoint, metadata, request, getIoTimeout()));
            }

            bool[] results = await Task.WhenAll(tasks);
            foreach (bool b in results)
            {
                if (!b)
                {
                    return false;
                }
            }
            return true;
        }

        protected readonly IClientManager Manager;
        
        private readonly INameServerResolver _nameServerResolver;
        private readonly CancellationTokenSource _nameServerResolverCts;
        private List<string> _nameServers;
        private int _currentNameServerIndex;

        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteTable;
        private readonly CancellationTokenSource _updateTopicRouteCts;

        private readonly CancellationTokenSource _healthCheckCts;

        protected const int MaxTransparentRetry = 3;
    }
}