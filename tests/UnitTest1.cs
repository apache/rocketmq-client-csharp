using Microsoft.VisualStudio.TestTools.UnitTesting;
using org.apache.rocketmq;
using Grpc.Net.Client;
using apache.rocketmq.v1;
namespace tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
        }

        [TestMethod]
        public void TestRpcClientImplCtor() {
            using var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new MessagingService.MessagingServiceClient(channel);
            RpcClientImpl impl = new RpcClientImpl(client);
        }
    }
}
