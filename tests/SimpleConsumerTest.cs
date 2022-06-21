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
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class SimpleConsumerTest
    {

        [TestMethod]
        public void TestStart()
        {
            var accessPoint = new AccessPoint();
            var host = "11.166.42.94";
            var port = 8081;
            accessPoint.Host = host;
            accessPoint.Port = port;
            var resourceNamespace = "";
            var group = "GID_cpp_sdk_standard";
            var topic = "cpp_sdk_standard";

            var simpleConsumer = new SimpleConsumer(accessPoint, resourceNamespace, group);
            simpleConsumer.Subscribe(topic, rmq::FilterType.Tag, "*");
            simpleConsumer.Start();
            Thread.Sleep(10_000);
        }

    }


}