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

using rmq = Apache.Rocketmq.V2;
using NLog;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class SimpleConsumer : Client
    {

        public SimpleConsumer(AccessPoint accessPoint,
        string resourceNamespace, string group)
        : base(accessPoint, resourceNamespace)
        {
            fifo_ = false;
            subscriptions_ = new ConcurrentDictionary<string, rmq.SubscriptionEntry>();
            group_ = group;
        }

        public override void BuildClientSetting(rmq::Settings settings)
        {
            base.BuildClientSetting(settings);

            settings.ClientType = rmq::ClientType.SimpleConsumer;
            settings.Subscription = new rmq::Subscription();
            settings.Subscription.Group = new rmq::Resource();
            settings.Subscription.Group.Name = Group;
            settings.Subscription.Group.ResourceNamespace = ResourceNamespace;

            foreach (var entry in subscriptions_)
            {
                settings.Subscription.Subscriptions.Add(entry.Value);
            }
        }

        public override void Start()
        {
            base.Start();
            base.createSession(_accessPoint.TargetUrl());
        }

        public override async Task Shutdown()
        {
            await base.Shutdown();
            if (!await NotifyClientTermination())
            {
                Logger.Warn("Failed to NotifyClientTermination");
            }
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {
            request.ClientType = rmq::ClientType.SimpleConsumer;
            request.Group = new rmq::Resource();
            request.Group.Name = Group;
            request.Group.ResourceNamespace = ResourceNamespace;
        }

        public void Subscribe(string topic, rmq::FilterType filterType, string expression)
        {
            var entry = new rmq::SubscriptionEntry();
            entry.Topic = new rmq::Resource();
            entry.Topic.Name = topic;
            entry.Topic.ResourceNamespace = ResourceNamespace;
            entry.Expression = new rmq::FilterExpression();
            entry.Expression.Type = filterType;
            entry.Expression.Expression = expression;
            subscriptions_.AddOrUpdate(topic, entry, (k, prev) => { return entry; });
        }

        public override void OnSettingsReceived(Settings settings)
        {
            base.OnSettingsReceived(settings);

            if (settings.Subscription.Fifo)
            {
                fifo_ = true;
            }

        }

        private string group_;

        public string Group
        {
            get { return group_; }
        }

        private bool fifo_;

        private ConcurrentDictionary<string, rmq::SubscriptionEntry> subscriptions_;

    }
}