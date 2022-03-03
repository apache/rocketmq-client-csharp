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

using rmq = Apache.Rocketmq.V1;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using System.Collections.Generic;
using System.Security.Cryptography;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public ClientManager()
        {
            _rpcClients = new Dictionary<string, RpcClient>();
            _clientLock = new ReaderWriterLockSlim();
        }

        public IRpcClient GetRpcClient(string target)
        {
            _clientLock.EnterReadLock();
            try
            {
                // client exists, return in advance.
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
                // client exists, return in advance.
                if (_rpcClients.ContainsKey(target))
                {
                    return _rpcClients[target];
                }

                // client does not exist, generate a new one
                var client = new RpcClient(target);
                _rpcClients.Add(target, client);
                return client;
            }
            finally
            {
                _clientLock.ExitWriteLock();
            }
        }

        public async Task<TopicRouteData> ResolveRoute(string target, grpc::Metadata metadata,
            rmq::QueryRouteRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
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
                Permission permission = Permission.ReadWrite;
                switch (partition.Permission)
                {
                    case rmq::Permission.None:
                    {
                        permission = Permission.None;
                        break;
                    }
                    case rmq::Permission.Read:
                    {
                        permission = Permission.Read;
                        break;
                    }
                    case rmq::Permission.Write:
                    {
                        permission = Permission.Write;
                        break;
                    }
                    case rmq::Permission.ReadWrite:
                    {
                        permission = Permission.ReadWrite;
                        break;
                    }
                }

                AddressScheme scheme = AddressScheme.IPv4;
                switch (partition.Broker.Endpoints.Scheme)
                {
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
                foreach (var item in partition.Broker.Endpoints.Addresses)
                {
                    addresses.Add(new Address(item.Host, item.Port));
                }

                ServiceAddress serviceAddress = new ServiceAddress(scheme, addresses);
                Broker broker = new Broker(partition.Broker.Name, id, serviceAddress);
                partitions.Add(new Partition(topic, broker, id, permission));
            }

            var topicRouteData = new TopicRouteData(partitions);
            return topicRouteData;
        }

        public async Task<Boolean> Heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request,
            TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.Heartbeat(metadata, request, timeout);
            Logger.Debug($"Heartbeat to {target} response status: {response.Common.Status.ToString()}");
            return response.Common.Status.Code == (int)Google.Rpc.Code.Ok;
        }

        public async Task<Boolean> HealthCheck(string target, grpc::Metadata metadata, rmq::HealthCheckRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            try
            {
                var response = await rpcClient.HealthCheck(metadata, request, timeout);
                return response.Common.Status.Code == (int)Google.Rpc.Code.Ok;
            }
            catch (System.Exception e)
            {
                Logger.Debug(e, $"Health-check to {target} failed");
                return false;
            }
        }

        public async Task<rmq::SendMessageResponse> SendMessage(string target, grpc::Metadata metadata,
            rmq::SendMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.SendMessage(metadata, request, timeout);
            return response;
        }

        public async Task<Boolean> NotifyClientTermination(string target, grpc::Metadata metadata,
            rmq::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            rmq::NotifyClientTerminationResponse response =
                await rpcClient.NotifyClientTermination(metadata, request, timeout);
            return response.Common.Status.Code == ((int)Google.Rpc.Code.Ok);
        }

        public async Task<List<rmq::Assignment>> QueryLoadAssignment(string target, grpc::Metadata metadata, rmq::QueryAssignmentRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            rmq::QueryAssignmentResponse response = await rpcClient.QueryAssignment(metadata, request, timeout);
            if (response.Common.Status.Code != (int)Google.Rpc.Code.Ok)
            {
                // TODO: Build exception hierarchy
                throw new Exception($"Failed to query load assignment from server. Cause: {response.Common.Status.Message}");
            }

            List<rmq::Assignment> assignments = new List<rmq.Assignment>();
            foreach (var item in response.Assignments)
            {
                assignments.Add(item);
            }
            return assignments;
        }

        public async Task<List<Message>> ReceiveMessage(string target, grpc::Metadata metadata, rmq::ReceiveMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            rmq::ReceiveMessageResponse response = await rpcClient.ReceiveMessage(metadata, request, timeout);
            if (response.Common.Status.Code != (int)Google.Rpc.Code.Ok)
            {
                throw new Exception($"Failed to receive messages from {target}, cause: {response.Common.Status.Message}");
            }

            List<Message> messages = new List<Message>();
            foreach (var message in response.Messages)
            {
                messages.Add(convert(target, message));
            }
            return messages;
        }

        private Message convert(string sourceHost, rmq::Message message)
        {
            var msg = new Message();
            msg.Topic = message.Topic.Name;
            msg.messageId = message.SystemAttribute.MessageId;
            msg.Tag = message.SystemAttribute.Tag;

            // Validate message body checksum
            byte[] raw = message.Body.ToByteArray();
            if (rmq::DigestType.Crc32 == message.SystemAttribute.BodyDigest.Type)
            {
                uint checksum = Force.Crc32.Crc32Algorithm.Compute(raw, 0, raw.Length);
                if (!message.SystemAttribute.BodyDigest.Checksum.Equals(checksum.ToString("X")))
                {
                    msg._bodyChecksumVerified = false;
                }
            }
            else if (rmq::DigestType.Md5 == message.SystemAttribute.BodyDigest.Type)
            {
                var checksum = MD5.HashData(raw);
                if (!message.SystemAttribute.BodyDigest.Checksum.Equals(Convert.ToHexString(checksum)))
                {
                    msg._bodyChecksumVerified = false;
                }
            }
            else if (rmq::DigestType.Sha1 == message.SystemAttribute.BodyDigest.Type)
            {
                var checksum = SHA1.HashData(raw);
                if (!message.SystemAttribute.BodyDigest.Checksum.Equals(Convert.ToHexString(checksum)))
                {
                    msg._bodyChecksumVerified = false;
                }
            }

            foreach (var entry in message.UserAttribute)
            {
                msg.UserProperties.Add(entry.Key, entry.Value);
            }

            msg._receiptHandle = message.SystemAttribute.ReceiptHandle;
            msg._sourceHost = sourceHost;

            foreach (var key in message.SystemAttribute.Keys)
            {
                msg.Keys.Add(key);
            }

            msg._deliveryAttempt = message.SystemAttribute.DeliveryAttempt;

            if (message.SystemAttribute.BodyEncoding == rmq::Encoding.Gzip)
            {
                // Decompress/Inflate message body
                var inputStream = new MemoryStream(message.Body.ToByteArray());
                var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
                var outputStream = new MemoryStream();
                gzipStream.CopyTo(outputStream);
                msg.Body = outputStream.ToArray();
            }
            else
            {
                msg.Body = message.Body.ToByteArray();
            }

            return msg;
        }

        public async Task<Boolean> Ack(string target, grpc::Metadata metadata, rmq::AckMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.AckMessage(metadata, request, timeout);
            return response.Common.Status.Code == ((int)Google.Rpc.Code.Ok);
        }

        public async Task<Boolean> Nack(string target, grpc::Metadata metadata, rmq::NackMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.NackMessage(metadata, request, timeout);
            return response.Common.Status.Code == ((int)Google.Rpc.Code.Ok);
        }

        public async Task Shutdown()
        {
            _clientLock.EnterReadLock();
            try
            {
                List<Task> tasks = new List<Task>();
                foreach (var item in _rpcClients)
                {
                    tasks.Add(item.Value.Shutdown());
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                _clientLock.ExitReadLock();
            }
        }

        private readonly Dictionary<string, RpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;
    }
}