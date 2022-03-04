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

namespace Org.Apache.Rocketmq {

    public class ClientConfig : IClientConfig {

        public ClientConfig() {
            var hostName = System.Net.Dns.GetHostName();
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.clientId_ = string.Format("{0}@{1}#{2}", hostName, pid, instanceName_);
            this._ioTimeout = TimeSpan.FromSeconds(3);
            this.longPollingIoTimeout_ = TimeSpan.FromSeconds(30);
        }

        public string region() {
            return _region;
        }
        public string Region {
            set { _region = value; }
        }

        public string serviceName() {
            return _serviceName;
        }
        public string ServiceName {
            set { _serviceName = value; }
        }

        public string resourceNamespace() {
            return _resourceNamespace;
        }
        public string ResourceNamespace {
            set { _resourceNamespace = value; }
        }

        public ICredentialsProvider credentialsProvider() {
            return credentialsProvider_;
        }
        
        public ICredentialsProvider CredentialsProvider {
            set { credentialsProvider_ = value; }
        }

        public string tenantId() {
            return _tenantId;
        }
        public string TenantId {
            set { _tenantId = value; }
        }

        public TimeSpan getIoTimeout() {
            return _ioTimeout;
        }
        public TimeSpan IoTimeout {
            set { _ioTimeout = value; }
        }

        public TimeSpan getLongPollingTimeout() {
            return longPollingIoTimeout_;
        }
        public TimeSpan LongPollingTimeout {
            set { longPollingIoTimeout_ = value; }
        }

        public string getGroupName() {
            return groupName_;
        }
        public string GroupName {
            set { groupName_ = value; }
        }

        public string clientId() {
            return clientId_;
        }

        public bool isTracingEnabled() {
            return tracingEnabled_;
        }
        public bool TracingEnabled {
            set { tracingEnabled_ = value; }
        }

        public void setInstanceName(string instanceName) {
            this.instanceName_ = instanceName;
        }

        private string _region = "cn-hangzhou";
        private string _serviceName = "ONS";

        protected string _resourceNamespace;

        private ICredentialsProvider credentialsProvider_;

        private string _tenantId;

        private TimeSpan _ioTimeout;

        private TimeSpan longPollingIoTimeout_;

        private string groupName_;

        private string clientId_;

        private bool tracingEnabled_ = false;

        private string instanceName_ = "default";
    }

}
