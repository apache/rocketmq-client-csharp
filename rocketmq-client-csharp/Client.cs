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
using Apache.Rocketmq.V1;
using grpc = global::Grpc.Core;
using NLog;


namespace Org.Apache.Rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public Client(INameServerResolver resolver, string resourceNamespace)
        {
            _nameServerResolver = resolver;
            _resourceNamespace = resourceNamespace;
            Manager = ClientManagerFactory.getClientManager(resourceNamespace);
            _nameServerResolverCts = new CancellationTokenSource();

            _topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            _updateTopicRouteCts = new CancellationTokenSource();
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
                    string target = partition.Broker.targetUrl();
                    if (acceptor(target))
                    {
                        return target;
                    }
                }
            }
            return null;
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

        protected Endpoints AccessEndpoint(string nameServer)
        {
            var endpoints = new Endpoints();
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
                var request = new QueryRouteRequest();
                request.Topic = new Resource();
                request.Topic.ResourceNamespace = _resourceNamespace;
                request.Topic.Name = topic;
                request.Endpoints = AccessEndpoint(nameServer);
                var metadata = new grpc.Metadata();
                Signature.sign(this, metadata);
                var topicRouteData = await Manager.ResolveRoute($"https://{nameServer}", metadata, request, getIoTimeout());
                if (null != topicRouteData)
                {
                    _topicRouteTable.TryAdd(topic, topicRouteData);

                    if (retry > 0)
                    {
                        _currentNameServerIndex = index;
                    }
                    return topicRouteData;
                }
            }
            return null;
        }

        public abstract void PrepareHeartbeatData(HeartbeatRequest request);

        public void Heartbeat()
        {
            List<string> endpoints = endpointsInUse();
            if (0 == endpoints.Count)
            {
                return;
            }

            var heartbeatRequest = new HeartbeatRequest();
            PrepareHeartbeatData(heartbeatRequest);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
        }

        public void HealthCheck()
        {

        }

        public async Task<List<Assignment>> scanLoadAssignment(string topic, string group)
        {
            // Pick a broker randomly
            string target = FilterBroker((s) => true);
            var request = new QueryAssignmentRequest();
            request.ClientId = clientId();
            request.Topic = new Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;
            request.Group = new Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;
            request.Endpoints = AccessEndpoint(_nameServers[_currentNameServerIndex]);
            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            return await Manager.QueryLoadAssignment(target, metadata, request, getIoTimeout());
        }

        public async Task<bool> NotifyClientTermination()
        {
            List<string> endpoints = endpointsInUse();
            var request = new NotifyClientTerminationRequest();
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

        private List<string> endpointsInUse()
        {
            //TODO: gather endpoints from route entries.
            return new List<string>();
        }

        protected readonly IClientManager Manager;
        
        private readonly INameServerResolver _nameServerResolver;
        private readonly CancellationTokenSource _nameServerResolverCts;
        private List<string> _nameServers;
        private int _currentNameServerIndex;

        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteTable;
        private readonly CancellationTokenSource _updateTopicRouteCts;

        protected const int MaxTransparentRetry = 3;
    }
}