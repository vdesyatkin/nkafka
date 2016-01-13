﻿using System;
using System.Collections.Generic;
using System.Text;
using NKafka.Client;
using NKafka.Client.Consumer;
using NKafka.Client.Producer;
// ReSharper disable ClassNeverInstantiated.Global

namespace NKafka.DevConsole
{
    class Program
    {
        static void Main()
        {
            var host = "192.168.137.196";
            var port = 9092;
            var metadataBroker = new KafkaBrokerInfo(host, port);
            var topicName = "test2";

            var clientConfigBuilder = new KafkaClientSettingsBuilder(metadataBroker)
                .SetClientId("nkafka")
                .SetKafkaVersion(KafkaVersion.V0_9)
                .SetWorkerThreadCount(1)
                .SetWorkerPeriod(TimeSpan.FromMilliseconds(500));
            var producerConfigBuilder = new KafkaProducerSettingsBuilder()
                .SetConsistencyLevel(KafkaConsistencyLevel.OneReplica)
                .SetProduceServerTimeout(TimeSpan.FromSeconds(5))
                .SetBatchMaxSizeBytes(10000);
            var consumerConfigBuilder = new KafkaConsumerSettingsBuilder()
                .SetBatchMinSizeBytes(1)
                .SetBatchMaxSizeBytes(10000)
                .SetConsumeServerWaitTime(TimeSpan.FromSeconds(5));            

            var clientBuilder = new KafkaClientBuilder(clientConfigBuilder.Build());
            var group = clientBuilder.CreateConsumerGroup("my_group");
            var topicProducer = clientBuilder.CreateTopicProducer(topicName,
                new TestPartitioner(), new TestSerializer(), producerConfigBuilder.Build());
            var topicConsumer = clientBuilder.CreateTopicConsumer(topicName, group,
                new TestSerializer(), consumerConfigBuilder.Build());
            var client = clientBuilder.Build();

            client.Start();

            string userText;
            do
            {
                Console.Write(">");
                userText = Console.ReadLine()?.ToLower().Trim();
                var data = userText?.Split(' ');
                if (data == null || data.Length < 1)
                {
                    Console.WriteLine("Inavlid command");
                    continue;
                }

                var command = data[0].Trim();
                if (command == "produce" || command == "p")
                {
                    if (data.Length < 3)
                    {
                        Console.WriteLine("Key and data required");
                        continue;
                    }
                    topicProducer.Produce(data[1].Trim(), data[2].Trim());
                }

                if (command == "consume" || command == "c")
                {
                    var package = topicConsumer.Consume();
                    if (package != null)
                    {
                        foreach (var message in package.Messages)
                        {
                            Console.WriteLine($"key={message.Key} data={message.Data}");
                        }
                        if (package.Messages.Count == 0)
                        {
                            Console.WriteLine("empty package");
                        }
                    }
                    else
                    {
                        Console.WriteLine("no packages");
                    }
                }
            } while (userText != "exit");

            Console.ReadLine();

            client.Stop();

            //var tester = new KafkaTester();
            //tester.Test(host, port, topicName);
            //Console.ReadLine();
        }

        private class TestSerializer : IKafkaProducerSerializer<string, string>, IKafkaConsumerSerializer<String, string>
        {
            public byte[] SerializeKey(string key)
            {
                return Encoding.UTF8.GetBytes(key);
            }

            public byte[] SerializeData(string value)
            {
                return Encoding.UTF8.GetBytes(value);
            }

            public string DeserializeKey(byte[] keyBytes)
            {
                return Encoding.UTF8.GetString(keyBytes);
            }

            public string DeserializeData(byte[] dataBytes)
            {
                return Encoding.UTF8.GetString(dataBytes);
            }
        }

        private class TestPartitioner : IKafkaProducerPartitioner<string, string>
        {            
            private readonly Random _rand = new Random();

            public int GetPartition(string key, string data, IReadOnlyList<int> partitions)
            {
                if (key == null) return 0;
                if (partitions.Count == 0) return 0;
                return partitions[_rand.Next(0, 100) % partitions.Count];
            }
        }
    }
}
