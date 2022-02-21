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
using rmq = apache.rocketmq.v1;
using grpc = global::Grpc.Core;
using NLog;


namespace org.apache.rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public Client(INameServerResolver resolver, string resourceNamespace)
        {
            this.nameServerResolver = resolver;
            this.resourceNamespace_ = resourceNamespace;
            this.clientManager = ClientManagerFactory.getClientManager(resourceNamespace);
            this.nameServerResolverCTS = new CancellationTokenSource();

            this.topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            this.updateTopicRouteCTS = new CancellationTokenSource();
        }

        public virtual void Start()
        {
            schedule(async () =>
            {
                await updateNameServerList();
            }, 30, nameServerResolverCTS.Token);

            schedule(async () =>
            {
                await updateTopicRoute();

            }, 30, updateTopicRouteCTS.Token);

        }

        public virtual async Task Shutdown()
        {
            Logger.Info($"Shutdown client[resource-namespace={resourceNamespace_}");
            updateTopicRouteCTS.Cancel();
            nameServerResolverCTS.Cancel();
            await clientManager.Shutdown();
        }

        private async Task updateNameServerList()
        {
            List<string> nameServers = await nameServerResolver.resolveAsync();
            if (0 == nameServers.Count)
            {
                // Whoops, something should be wrong. We got an empty name server list.
                Logger.Warn("Got an empty name server list");
                return;
            }

            if (nameServers.Equals(this.nameServers))
            {
                Logger.Debug("Name server list remains unchanged");
                return;
            }

            // Name server list is updated. 
            // TODO: Locking is required
            this.nameServers = nameServers;
            this.currentNameServerIndex = 0;
        }

        private async Task updateTopicRoute()
        {
            if (null == nameServers || 0 == nameServers.Count)
            {
                List<string> list = await nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this.nameServers = list;
                }
                else
                {
                    Logger.Error("Failed to resolve name server list");
                    return;
                }
            }

            // We got one or more name servers available.
            string nameServer = nameServers[currentNameServerIndex];

            List<Task<TopicRouteData>> tasks = new List<Task<TopicRouteData>>();
            foreach (var item in topicRouteTable)
            {
                tasks.Add(getRouteFor(item.Key, true));
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
                var existing = topicRouteTable[topicName];
                if (!existing.Equals(item))
                {
                    topicRouteTable[topicName] = item;
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

        /**
         * Parameters:
         * topic
         *    Topic to query
         * direct
         *    Indicate if we should by-pass cache and fetch route entries from name server.
         */
        public async Task<TopicRouteData> getRouteFor(string topic, bool direct)
        {
            if (!direct && topicRouteTable.ContainsKey(topic))
            {
                return topicRouteTable[topic];
            }

            if (null == nameServers || 0 == nameServers.Count)
            {
                List<string> list = await nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this.nameServers = list;
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
                int index = (currentNameServerIndex + retry) % nameServers.Count;
                string nameServer = nameServers[index];
                var request = new rmq::QueryRouteRequest();
                request.Topic = new rmq::Resource();
                request.Topic.ResourceNamespace = resourceNamespace_;
                request.Topic.Name = topic;
                request.Endpoints = new rmq::Endpoints();
                request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
                var address = new rmq::Address();
                int pos = nameServer.LastIndexOf(':');
                address.Host = nameServer.Substring(0, pos);
                address.Port = Int32.Parse(nameServer.Substring(pos + 1));
                request.Endpoints.Addresses.Add(address);
                var target = string.Format("https://{0}:{1}", address.Host, address.Port);
                var metadata = new grpc.Metadata();
                Signature.sign(this, metadata);
                var topicRouteData = await clientManager.resolveRoute(target, metadata, request, getIoTimeout());
                if (null != topicRouteData)
                {
                    if (retry > 0)
                    {
                        currentNameServerIndex = index;
                    }
                    return topicRouteData;
                }
            }
            return null;
        }

        public abstract void prepareHeartbeatData(rmq::HeartbeatRequest request);

        public void heartbeat()
        {
            List<string> endpoints = endpointsInUse();
            if (0 == endpoints.Count)
            {
                return;
            }

            var heartbeatRequest = new rmq::HeartbeatRequest();
            prepareHeartbeatData(heartbeatRequest);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
        }

        public void healthCheck()
        {

        }

        public async Task<bool> notifyClientTermination()
        {
            List<string> endpoints = endpointsInUse();
            var request = new rmq::NotifyClientTerminationRequest();
            request.ClientId = clientId();

            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);

            List<Task<Boolean>> tasks = new List<Task<Boolean>>();

            foreach (var endpoint in endpoints)
            {
                tasks.Add(clientManager.notifyClientTermination(endpoint, metadata, request, getIoTimeout()));
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

        protected IClientManager clientManager;
        private INameServerResolver nameServerResolver;
        private CancellationTokenSource nameServerResolverCTS;
        private List<string> nameServers;
        private int currentNameServerIndex;

        private ConcurrentDictionary<string, TopicRouteData> topicRouteTable;
        private CancellationTokenSource updateTopicRouteCTS;

        protected const int MaxTransparentRetry = 3;
    }
}