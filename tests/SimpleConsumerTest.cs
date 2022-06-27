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
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using rmq = Apache.Rocketmq.V2;
using System.Threading.Tasks;
using Org.Apache.Rocketmq;

namespace tests
{

    [TestClass]
    public class SimpleConsumerTest
    {

        private static AccessPoint accessPoint;
        private static string _resourceNamespace = "";
        private static string _group = "GID_cpp_sdk_standard";
        private static string _topic = "cpp_sdk_standard";


        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            accessPoint = new AccessPoint
            {
                Host = "127.0.0.1",
                Port = 8081
            };
        }

        [TestMethod]
        public async Task TestLifecycle()
        {
            var simpleConsumer = new SimpleConsumer(accessPoint, _resourceNamespace, _group);
            simpleConsumer.Subscribe(_topic, rmq::FilterType.Tag, "*");
            await simpleConsumer.Start();
            Thread.Sleep(1_000);
            await simpleConsumer.Shutdown();
        }

        [TestMethod]
        public async Task TestReceive()
        {
            var simpleConsumer = new SimpleConsumer(accessPoint, _resourceNamespace, _group);
            simpleConsumer.Subscribe(_topic, rmq::FilterType.Tag, "*");
            await simpleConsumer.Start();
            var batchSize = 32;
            var receiveTimeout = TimeSpan.FromSeconds(10);
            var messages  = await simpleConsumer.Receive(batchSize, receiveTimeout);
            Console.WriteLine($"Received {messages.Count} messages in all");
            await simpleConsumer.Shutdown();
        }
    }
}