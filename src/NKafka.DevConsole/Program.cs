using System;
using System.Text;
using NKafka.Producer;

namespace NKafka.DevConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = "192.168.137.196";
            var port = 9092;

            var metadataBroker = new KafkaBrokerInfo(host, port);
            var configBuilder = new KafkaProducerSettingsBuilder(metadataBroker);
            configBuilder.SetClientId("nkafka").SetProduceTimeout(TimeSpan.FromSeconds(5));
            var producerBuilder = new KafkaProducerBuilder();
            var topic = producerBuilder.AddTopic("test", new TestPartitioner(), new TestSerializer());
            var producer = producerBuilder.Build(configBuilder);

            producer.Start();

            if (topic != null)
            {
                topic.EnqueueMessage("1", "12345");
                topic.EnqueueMessage("2", "Вышел зайчик погулять");
            }

            Console.ReadLine();

            producer.Stop();

            Console.ReadLine();

            var tester = new KafkaTester();
            tester.Test(host, port, "test");
            Console.ReadLine();
        }

        private class TestSerializer : IKafkaProducerSerializer<string, string>
        {
            public byte[] SerializeKey(string key)
            {
                return Encoding.UTF8.GetBytes(key);
            }

            public byte[] SerializeValue(string value)
            {
                return Encoding.UTF8.GetBytes(value);
            }
        }

        private class TestPartitioner : IKafkaProducerPartitioner<string, string>
        {
            public int GetPartitionIndex(string key, string data, int partitionCount)
            {
                if (key == null) return 0;
                if (partitionCount == 0) return 0;
                return key.GetHashCode() % partitionCount;
            }
        }
    }
}
