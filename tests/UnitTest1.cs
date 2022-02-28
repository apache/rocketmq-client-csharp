using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Grpc.Net.Client;
using Apache.Rocketmq.V1;

using System;
namespace tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            Apache.Rocketmq.V1.Permission perm = Apache.Rocketmq.V1.Permission.None;
            switch(perm) {
                case Apache.Rocketmq.V1.Permission.None:
                {
                    Console.WriteLine("None");
                    break;
                }

                case Apache.Rocketmq.V1.Permission.Read:
                {
                    Console.WriteLine("Read");
                    break;
                }

                case Apache.Rocketmq.V1.Permission.Write:
                {
                    Console.WriteLine("Write");
                    break;
                }

                case Apache.Rocketmq.V1.Permission.ReadWrite:
                {
                    Console.WriteLine("ReadWrite");
                    break;
                }

            }
        }

        [TestMethod]
        public void TestRpcClientImplCtor() {
            RpcClient impl = new RpcClient("https://localhost:5001");
        }
    }
}
