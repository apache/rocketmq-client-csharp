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
using rmq = Apache.Rocketmq.V2;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Google.Protobuf;
using Grpc.Core;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class Producer : Client, IProducer
    {
        public Producer(AccessPoint accessPoint, string resourceNamespace) : base(accessPoint, resourceNamespace)
        {
            this.loadBalancer = new ConcurrentDictionary<string, PublishLoadBalancer>();
        }

        public override void Start()
        {
            base.Start();
            // More initialization
        }

        public override void Shutdown()
        {
            // Release local resources
            base.Shutdown();
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {

        }

        public async Task<SendReceipt> Send(Message message)
        {
            if (!loadBalancer.ContainsKey(message.Topic))
            {
                var topicRouteData = await GetRouteFor(message.Topic, false);
                if (null == topicRouteData || null == topicRouteData.MessageQueues || 0 == topicRouteData.MessageQueues.Count)
                {
                    Logger.Error($"Failed to resolve route info for {message.Topic}");
                    throw new TopicRouteException(string.Format("No topic route for {0}", message.Topic));
                }

                var loadBalancerItem = new PublishLoadBalancer(topicRouteData);
                loadBalancer.TryAdd(message.Topic, loadBalancerItem);
            }

            var publishLB = loadBalancer[message.Topic];

            var request = new rmq::SendMessageRequest();
            var entry = new rmq::Message();
            entry.Body = ByteString.CopyFrom(message.Body);
            entry.Topic = new rmq::Resource();
            entry.Topic.ResourceNamespace = resourceNamespace();
            entry.Topic.Name = message.Topic;
            request.Messages.Add(entry);

            // User properties
            foreach (var item in message.UserProperties)
            {
                entry.UserProperties.Add(item.Key, item.Value);
            }

            entry.SystemProperties = new rmq::SystemProperties();
            entry.SystemProperties.MessageId = message.MessageId;
            if (!string.IsNullOrEmpty(message.Tag))
            {
                entry.SystemProperties.Tag = message.Tag;
            }

            if (0 != message.Keys.Count)
            {
                foreach (var key in message.Keys)
                {
                    entry.SystemProperties.Keys.Add(key);
                }
            }

            // string target = "https://";
            List<string> targets = new List<string>();
            List<rmq::MessageQueue> candidates = publishLB.select(message.MaxAttemptTimes);
            foreach (var messageQueue in candidates)
            {
                targets.Add(Utilities.TargetUrl(messageQueue));
            }

            var metadata = new Metadata();
            Signature.sign(this, metadata);

            Exception ex = null;

            foreach (var target in targets)
            {
                try
                {
                    rmq::SendMessageResponse response = await Manager.SendMessage(target, metadata, request, RequestTimeout);
                    if (null != response && rmq::Code.Ok == response.Status.Code)
                    {

                        var messageId = response.Entries[0].MessageId;
                        return new SendReceipt(messageId);
                    }
                }
                catch (Exception e)
                {
                    Logger.Info(e, $"Failed to send message to {target}");
                    ex = e;
                }
            }

            if (null != ex)
            {
                Logger.Error(ex, $"Failed to send message after {message.MaxAttemptTimes} attempts");
                throw ex;
            }

            Logger.Error($"Failed to send message after {message.MaxAttemptTimes} attempts with unspecified reasons");
            throw new Exception("Send message failed");
        }

        private ConcurrentDictionary<string, PublishLoadBalancer> loadBalancer;
        private static new readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
    }
}