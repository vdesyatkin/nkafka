using System;
using System.Collections.Generic;
using System.Text;
using NKafka.Client;
using NKafka.Client.Producer;

namespace NKafka.DevConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = "192.168.137.196";
            var port = 9092;
            //var metadataBroker = new KafkaBrokerInfo(host, port);

            //var configBuilder = new KafkaClientSettingsBuilder(metadataBroker);
            //configBuilder.SetClientId("nkafka");
            //configBuilder.Producer.SetProduceServerTimeout(TimeSpan.FromSeconds(5));

            //var clientBuilder = new KafkaClientBuilder();
            //var topicProducer = clientBuilder.CreateTopicProducer("test2", new TestPartitioner(), new TestSerializer());
            //var client = clientBuilder.Build(configBuilder);

            //client.Start();

            //topicProducer.EnqueueMessage("1", "12345");
            //topicProducer.EnqueueMessage("2", "Вышел зайчик погулять");

            //Console.ReadLine();

            //client.Stop();

            //Console.ReadLine();

            var tester = new KafkaTester();
            tester.Test(host, port, "test2");
            Console.ReadLine();
        }

        private class TestSerializer : IKafkaProducerSerializer<string, string>
        {
            public byte[] SerializeKey(string key)
            {
                return Encoding.UTF8.GetBytes(key);
            }

            public byte[] SerializeData(string value)
            {
                return Encoding.UTF8.GetBytes(value);
            }
        }

        private class TestPartitioner : IKafkaProducerPartitioner<string, string>
        {            
            public int GetPartition(string key, string data, IReadOnlyList<int> partitions)
            {
                if (key == null) return 0;
                if (partitions.Count == 0) return 0;
                return partitions[key.GetHashCode() % partitions.Count];
            }
        }
    }
}
