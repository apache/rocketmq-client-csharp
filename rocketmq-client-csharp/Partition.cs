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

namespace Org.Apache.Rocketmq
{
    public class Partition : IEquatable<Partition>, IComparable<Partition>
    {
        public Partition(Topic topic, Broker broker, int id, Permission permission)
        {
            Topic = topic;
            Broker = broker;
            Id = id;
            Permission = permission;
        }

        public Topic Topic { get; }
        public Broker Broker { get; }
        public int Id { get; }

        public Permission Permission { get; }

        public bool Equals(Partition other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Equals(Topic, other.Topic) && Equals(Broker, other.Broker) && Id == other.Id &&
                   Permission == other.Permission;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj) || obj.GetType() != GetType())
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return Equals((Partition)obj);
        }


        public override int GetHashCode()
        {
            return HashCode.Combine(Topic, Broker, Id, (int)Permission);
        }

        public int CompareTo(Partition other)
        {
            if (ReferenceEquals(null, other))
            {
                return -1;
            }

            var compareTo = Topic.CompareTo(other.Topic);
            if (0 == compareTo)
            {
                compareTo = Broker.CompareTo(other.Broker);
            }

            if (0 == compareTo)
            {
                compareTo = Id.CompareTo(other.Id);
            }

            if (0 == compareTo)
            {
                compareTo = Permission.CompareTo(other.Permission);
            }

            return compareTo;
        }
    }
}