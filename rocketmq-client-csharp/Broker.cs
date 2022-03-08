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
    public class Broker : IComparable<Broker>, IEquatable<Broker>
    {
        public Broker(string name, int id, ServiceAddress address)
        {
            Name = name;
            Id = id;
            Address = address;
        }

        public string Name { get; }
        public int Id { get; }
        public ServiceAddress Address { get; }

        /// <summary>
        /// Calculate context aware primary target URL.
        /// </summary>
        /// <returns>Context aware primary target URL.</returns>
        public string TargetUrl()
        {
            var address = Address.Addresses[0];
            return $"https://{address.Host}:{address.Port}";
        }

        /// <summary>
        /// Judge whether equals to other or not, ignore <see cref="Address"/> on purpose.
        /// </summary>
        public bool Equals(Broker other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Name == other.Name && Id == other.Id;
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

            return Equals((Broker)obj);
        }

        /// <summary>
        /// Return the hash code, ignore <see cref="Address"/> on purpose.
        /// </summary>
        public override int GetHashCode()
        {
            return HashCode.Combine(Name, Id);
        }

        /// <summary>
        /// Compare with other, ignore <see cref="Address"/> on purpose.
        /// </summary>
        public int CompareTo(Broker other)
        {
            if (ReferenceEquals(null, other))
            {
                return -1;
            }

            var compareTo = String.CompareOrdinal(Name, other.Name);
            if (0 == compareTo)
            {
                compareTo = Id.CompareTo(other.Id);
            }

            return compareTo;
        }
    }
}