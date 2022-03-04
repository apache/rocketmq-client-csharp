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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using rmq = global::Apache.Rocketmq.V1;
using grpc = global::Grpc.Core;
using System;
using pb = global::Google.Protobuf;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    [TestClass]
    public class RpcClientTest
    {


        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            string target = string.Format("https://{0}:{1}", host, port);
            rpcClient = new RpcClient(target);

            clientConfig = new ClientConfig();
            var credentialsProvider = new ConfigFileCredentialsProvider();
            clientConfig.CredentialsProvider = credentialsProvider;
            clientConfig.ResourceNamespace = resourceNamespace;
            clientConfig.Region = "cn-hangzhou-pre";
        }

        [ClassCleanup]
        public static void TearDown()
        {

        }

        [TestMethod]
        public void testQueryRoute()
        {
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = resourceNamespace;
            request.Topic.Name = topic;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Host = host;
            address.Port = port;
            request.Endpoints.Addresses.Add(address);

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            // Execute route query multiple times.
            List<Task<rmq::QueryRouteResponse>> tasks = new List<Task<rmq.QueryRouteResponse>>();
            for (int i = 0; i < 16; i++)
            {
                tasks.Add(rpcClient.QueryRoute(metadata, request, clientConfig.getIoTimeout()));
            }

            // Verify
            var result = Task.WhenAll(tasks).GetAwaiter().GetResult();
            foreach (var item in result)
            {
                Assert.AreEqual(0, item.Common.Status.Code);
            }
        }


        [TestMethod]
        public void testHeartbeat()
        {
            var request = new rmq::HeartbeatRequest();
            request.ClientId = clientId;
            request.ProducerData = new rmq::ProducerData();
            request.ProducerData.Group = new rmq::Resource();
            request.ProducerData.Group.ResourceNamespace = resourceNamespace;
            request.ProducerData.Group.Name = topic;
            request.FifoFlag = false;

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);
            
            var response = rpcClient.Heartbeat(metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
            Assert.AreEqual("ok", response.Common.Status.Message);
        }

        [TestMethod]
        public void testSendMessage()
        {
            var request = new rmq::SendMessageRequest();
            request.Message = new rmq::Message();
            byte[] body = new byte[1024];
            for (int i = 0; i < body.Length; i++)
            {
                body[i] = (byte)'x';
            }
            request.Message.Body = pb::ByteString.CopyFrom(body);
            request.Message.Topic = new rmq::Resource();
            request.Message.Topic.ResourceNamespace = resourceNamespace;
            request.Message.Topic.Name = topic;
            request.Message.UserAttribute.Add("k", "v");
            request.Message.UserAttribute.Add("key", "value");
            request.Message.SystemAttribute = new rmq::SystemAttribute();
            request.Message.SystemAttribute.Tag = "TagA";
            request.Message.SystemAttribute.Keys.Add("key1");
            request.Message.SystemAttribute.MessageId = SequenceGenerator.Instance.Next();

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var response = rpcClient.SendMessage(metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
        }

        [TestMethod]
        public void testHealthCheck()
        {
            var request = new rmq::HealthCheckRequest();
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = resourceNamespace;
            request.Group.Name = group;
            request.ClientHost = "test";
            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);
            var response = rpcClient.HealthCheck(metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
            Assert.AreEqual("ok", response.Common.Status.Message);
        }

        [TestMethod]
        public void HeartbeatAsConsumer()
        {
            var request = new rmq::HeartbeatRequest();
            request.ClientId = clientId;
            request.ConsumerData = new rmq::ConsumerData();
            request.ConsumerData.Group = new rmq::Resource();
            request.ConsumerData.Group.ResourceNamespace = resourceNamespace;
            request.ConsumerData.Group.Name = group;

            request.ConsumerData.ConsumeModel = rmq::ConsumeModel.Clustering;
            request.ConsumerData.ConsumePolicy = rmq::ConsumePolicy.Resume;
            request.ConsumerData.ConsumeType = rmq::ConsumeMessageType.Passive;

            var subscription = new rmq::SubscriptionEntry();
            subscription.Topic = new rmq::Resource();
            subscription.Topic.ResourceNamespace = resourceNamespace;
            subscription.Topic.Name = topic;
            subscription.Expression = new rmq::FilterExpression();
            subscription.Expression.Type = rmq::FilterType.Tag;
            subscription.Expression.Expression = "*";
            request.ConsumerData.Subscriptions.Add(subscription);

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var response = rpcClient.Heartbeat(metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
            Assert.AreEqual("ok", response.Common.Status.Message);
        }

        private rmq::QueryAssignmentResponse queryAssignments()
        {
            HeartbeatAsConsumer();
            var request = new rmq::QueryAssignmentRequest();
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Host = host;
            address.Port = port;
            request.Endpoints.Addresses.Add(address);

            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = resourceNamespace;
            request.Group.Name = group;

            request.ClientId = clientId;
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = resourceNamespace;
            request.Topic.Name = topic;

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var response = rpcClient.QueryAssignment(metadata, request, System.TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
            return response;
        }

        [TestMethod]
        public void testQueryAssignment()
        {
            var response = queryAssignments();

            Assert.AreEqual("ok", response.Common.Status.Message);
            Assert.IsTrue(response.Assignments.Count > 0);
        }

        private rmq::ReceiveMessageResponse DoReceiveMessage()
        {
            var assignmentsResponse = queryAssignments();
            Assert.IsTrue(assignmentsResponse.Assignments.Count > 0);
            var assignment = assignmentsResponse.Assignments[0];

            // Send some prior messages 
            for (int i = 0; i < batchSize; i++)
            {
                testSendMessage();
            }

            var request = new rmq::ReceiveMessageRequest();
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = resourceNamespace;
            request.Group.Name = group;

            request.ClientId = clientId;
            request.Partition = assignment.Partition;

            request.FilterExpression = new rmq::FilterExpression();
            request.FilterExpression.Type = rmq::FilterType.Tag;
            request.FilterExpression.Expression = "*";

            request.ConsumePolicy = rmq::ConsumePolicy.Resume;
            request.BatchSize = batchSize;

            request.InvisibleDuration = new Google.Protobuf.WellKnownTypes.Duration();
            request.InvisibleDuration.Seconds = 10;
            request.InvisibleDuration.Nanos = 0;

            request.AwaitTime = new Google.Protobuf.WellKnownTypes.Duration();
            request.AwaitTime.Seconds = 10;
            request.AwaitTime.Nanos = 0;

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);
            var response = rpcClient.ReceiveMessage(metadata, request, TimeSpan.FromSeconds(15)).GetAwaiter().GetResult();
            return response;
        }

        [TestMethod]
        public void testReceiveMessage()
        {
            var response = DoReceiveMessage();
            Assert.AreEqual(0, response.Common.Status.Code);
            Assert.IsTrue(response.Messages.Count > 0);
        }

        [TestMethod]
        public void testAck()
        {
            var receiveMessageResponse = DoReceiveMessage();

            List<Task<rmq::AckMessageResponse>> tasks = new List<Task<rmq.AckMessageResponse>>();

            foreach (var message in receiveMessageResponse.Messages)
            {
                var request = new rmq::AckMessageRequest();
                request.Topic = new rmq::Resource();
                request.Topic.ResourceNamespace = resourceNamespace;
                request.Topic.Name = topic;

                request.Group = new rmq::Resource();
                request.Group.ResourceNamespace = resourceNamespace;
                request.Group.Name = group;

                request.ClientId = clientId;

                request.ReceiptHandle = message.SystemAttribute.ReceiptHandle;
                request.MessageId = message.SystemAttribute.MessageId;
                var metadata = new grpc::Metadata();
                Signature.sign(clientConfig, metadata);
                tasks.Add(rpcClient.AckMessage(metadata, request, TimeSpan.FromSeconds(3)));
            }
            var result = Task.WhenAll(tasks).GetAwaiter().GetResult();
            foreach (var item in result)
            {
                Assert.AreEqual("ok", item.Common.Status.Message);
            }
        }

        [TestMethod]
        public void testNack()
        {
            var receiveMessageResponse = DoReceiveMessage();
            List<Task<rmq::NackMessageResponse>> tasks = new List<Task<rmq.NackMessageResponse>>();
            foreach (var message in receiveMessageResponse.Messages)
            {
                var request = new rmq::NackMessageRequest();
                request.Topic = new rmq::Resource();
                request.Topic.ResourceNamespace = resourceNamespace;
                request.Topic.Name = topic;

                request.Group = new rmq::Resource();
                request.Group.ResourceNamespace = resourceNamespace;
                request.Group.Name = group;

                request.ClientId = clientId;

                request.ReceiptHandle = message.SystemAttribute.ReceiptHandle;
                request.MessageId = message.SystemAttribute.MessageId;
                request.DeliveryAttempt = 1;
                request.MaxDeliveryAttempts = 16;

                var metadata = new grpc::Metadata();
                Signature.sign(clientConfig, metadata);
                tasks.Add(rpcClient.NackMessage(metadata, request, TimeSpan.FromSeconds(3)));
            }
            var result = Task.WhenAll(tasks).GetAwaiter().GetResult();
            foreach (var item in result)
            {
                Assert.AreEqual(0, item.Common.Status.Code);
            }
        }

        // Remove the Ignore annotation if server has fixed
        [Ignore]
        [TestMethod]
        public void testNotifyClientTermiantion()
        {
            var request = new rmq::NotifyClientTerminationRequest();
            request.ClientId = clientId;
            request.ProducerGroup = new rmq::Resource();
            request.ProducerGroup.ResourceNamespace = resourceNamespace;
            request.ProducerGroup.Name = group;

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var response = rpcClient.NotifyClientTermination(metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
        }

        private static string resourceNamespace = "MQ_INST_1080056302921134_BXuIbML7";

        private static string topic = "cpp_sdk_standard";

        private static string clientId = "C001";
        private static string group = "GID_cpp_sdk_standard";

        private static string host = "116.62.231.199";
        private static int port = 80;

        private static int batchSize = 32;

        private static IRpcClient rpcClient;

        private static ClientConfig clientConfig;
    }
}