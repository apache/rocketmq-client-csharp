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

using System.Collections.Generic;

namespace Org.Apache.Rocketmq
{
    // Settings for publishing
    public class Publishing
    {
        private List<rmq::Resource> _topics;
        public List<rmq::Resource> Topics
        {
            get { return _topics; }
            set { _topics = value; }
        }

        private int _compressBodyThreshold;
        public int CompressBodyThreshold
        {
            get { return _compressBodyThreshold; }
            set { _compressBodyThreshold = value; }
        }

        private int _maxBodySize;
        public int MaxBodySize
        {
            get { return _maxBodySize; }
            set { _maxBodySize = value; }
        }

    }


}