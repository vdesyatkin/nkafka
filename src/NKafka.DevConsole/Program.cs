﻿using System;
using System.Collections.Generic;
using System.Text;
using JetBrains.Annotations;
using NKafka.Client;
using NKafka.Client.Consumer;
using NKafka.Client.ConsumerGroup;
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
            var groupName = "group61";

            //var tester = new KafkaTester();
            //tester.Test(host, port, topicName);
            //Console.ReadLine();
            //return;

            var clientConfigBuilder = new KafkaClientSettingsBuilder(metadataBroker)
                .SetClientId("nkafka")
                .SetKafkaVersion(KafkaVersion.V0_10)
                .SetWorkerThreadCount(1)
                .SetWorkerPeriod(TimeSpan.FromMilliseconds(500));
            var producerConfigBuilder = new KafkaProducerSettingsBuilder()
                .SetConsistencyLevel(KafkaConsistencyLevel.OneReplica)
                .SetCodecType(KafkaCodecType.CodecGzip)
                .SetBatchServerTimeout(TimeSpan.FromSeconds(5))
                .SetBatchSizeByteCount(10000);
            var consumerConfigBuilder = new KafkaConsumerSettingsBuilder()
                .SetBatchMinSizeBytes(1)
                .SetBatchMaxSizeBytes(10000)
                .SetConsumeServerWaitTime(TimeSpan.FromSeconds(5));

            var clientBuilder = new KafkaClientBuilder(clientConfigBuilder.Build());
            var group = clientBuilder.CreateConsumerGroup(groupName, KafkaConsumerGroupType.BalancedConsumers);
            var topicProducer = clientBuilder.CreateTopicProducer(topicName,
                new TestPartitioner(), new TestSerializer(), null, null, producerConfigBuilder.Build());
            var topicConsumer = clientBuilder.CreateTopicConsumer(topicName, group,
                new TestSerializer(), null, consumerConfigBuilder.Build());
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
                    topicProducer.EnqueueMessage(data[1].Trim(), data[2].Trim(), DateTime.UtcNow);
                }                

                if (command == "consume" || command == "c")
                {
                    var packages = topicConsumer.Consume();
                    if (packages.Count > 0)
                    {
                        foreach (var package in packages)
                        {
                            foreach (var message in package.Messages)
                            {
                                Console.WriteLine($"key={message.Key} data={message.Data}");
                            }
                            topicConsumer.TryEnqueueCommit(package.PackageId);
                        }                                              
                    }
                    else
                    {
                        Console.WriteLine("no packages");
                    }                   
                }

                if (command == "produceinfo" || command == "pi")
                {
                    var info = topicProducer?.GetDiagnosticsInfo();
                    if (info != null)
                    {
                        Console.WriteLine(info.IsReady);
                    }
                }

                if (command == "consumeinfo" || command == "ci")
                {
                    var info = topicConsumer.GetDiagnosticsInfo();
                    if (info != null)
                    {
                        Console.WriteLine(info.IsReady);
                    }
                }

                if (command == "groupinfo" || command == "gi")
                {
                    var info = group?.GetDiagnosticsInfo();
                    if (info != null)
                    {
                        Console.WriteLine(info.IsReady);
                    }
                }

            } while (userText != "exit" && userText != "q" && userText != "quit");            

            Console.WriteLine("flushing...");

            var isFlushed = client.TryPauseAndFlush(TimeSpan.FromSeconds(10));
            if (!isFlushed)
            {
                Console.WriteLine("not flushed!");                
            }
            else
            {
                Console.WriteLine("flushed");
            }

            Console.WriteLine("stopping...");
            client.Stop();
            Console.WriteLine("stopped");

            Console.ReadLine();            
        }       

        private class TestSerializer : IKafkaSerializer<string, string>
        {
            public KafkaMessage SerializeMessage(KafkaMessage<string, string> message)
            {
                var key = message.Key != null ? Encoding.UTF8.GetBytes(message.Key) : null;
                var data = message.Data != null ? Encoding.UTF8.GetBytes(message.Data) : null;
                return new KafkaMessage(key, data, message.TiemestampUtc);
            }

            public KafkaMessage<string, string> DeserializeMessage(KafkaMessage message)
            {
                var key = message.Key != null ? Encoding.UTF8.GetString(message.Key) : null;
                var data = message.Data != null ? Encoding.UTF8.GetString(message.Data) : null;
                return new KafkaMessage<string, string>(key, data, message.TiemestampUtc);
            }
        }

        private class TestPartitioner : IKafkaProducerPartitioner<string, string>
        {            
            [NotNull] private readonly Random _rand = new Random();

            public int GetPartition(KafkaMessage<string, string> message, IReadOnlyList<int> partitions)
            {                
                if (partitions.Count == 0) return 0;

                int intKey;
                if (message.Key != null && int.TryParse(message.Key, out intKey))
                {
                    return partitions[intKey%partitions.Count];
                }

                return partitions[_rand.Next(0, 100) % partitions.Count];
            }
        }        
    }
}
