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
using System.Threading.Tasks;
using grpc = global::Grpc.Core;
using NLog;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    class Session
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public Session(string target,
            grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> stream,
            IClient client)
        {
            this._target = target;
            this._stream = stream;
            this._client = client;
        }

        public async Task Loop()
        {
            var reader = this._stream.ResponseStream;
            var writer = this._stream.RequestStream;
            var request = new rmq::TelemetryCommand();
            request.Settings = new rmq::Settings();
            _client.BuildClientSetting(request.Settings);
            await writer.WriteAsync(request);
            Logger.Debug($"Writing Client Settings Done: {request.Settings.ToString()}");
            while (!_cts.IsCancellationRequested)
            {
                if (await reader.MoveNext(_cts.Token))
                {
                    var cmd = reader.Current;
                    Logger.Debug($"Received a TelemetryCommand: {cmd.ToString()}");
                    switch (cmd.CommandCase)
                    {
                        case rmq::TelemetryCommand.CommandOneofCase.None:
                            {
                                Logger.Warn($"Telemetry failed: {cmd.Status}");
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.Settings:
                            {
                                Logger.Info($"Received settings from server {cmd.Settings.ToString()}");
                                _client.OnReceive(cmd.Settings);
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.PrintThreadStackTraceCommand:
                            {
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand:
                            {
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.VerifyMessageCommand:
                            {
                                break;
                            }
                    }
                }
            }
        }

        public void Cancel()
        {
            _cts.Cancel();
        }

        private string _target;

        public string Target
        {
            get { return _target; }
        }

        private grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> _stream;
        private IClient _client;

        private CancellationTokenSource _cts = new CancellationTokenSource();

        public CancellationTokenSource CTS
        {
            get { return _cts; }
        }
    };
}