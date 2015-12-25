﻿using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerTopic
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly ConcurrentDictionary<int, KafkaConsumerBrokerPartition> Partitions;

        public KafkaConsumerBrokerTopic([NotNull]string topicName)
        {
            TopicName = topicName;
            Partitions = new ConcurrentDictionary<int, KafkaConsumerBrokerPartition>();
        }
    }
}
