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
using System;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class ProducerTest
    {

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            credentialsProvider = new ConfigFileCredentialsProvider();
        }

        [ClassCleanup]
        public static void TearDown()
        {

        }

        [TestMethod]
        public async Task testSendMessage()
        {
            var accessPoint = new AccessPoint();
            accessPoint.Host = host;
            accessPoint.Port = port;
            var producer = new Producer(accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            var sendResult = await producer.Send(msg);
            Assert.IsNotNull(sendResult);
            await producer.Shutdown();
        }

        private static string resourceNamespace = "";

        private static string topic = "cpp_sdk_standard";

        private static ICredentialsProvider credentialsProvider;
        private static string host = "11.166.42.94";
        private static int port = 8081;
    }

}