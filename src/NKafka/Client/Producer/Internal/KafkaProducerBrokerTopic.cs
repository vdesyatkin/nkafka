﻿using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerTopic
    {
        [NotNull] public readonly string TopicName;

        [NotNull] public readonly KafkaProducerSettings Settings;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaProducerBrokerPartition> Partitions;

        public int ProducePartitionIndex;

        public KafkaConsistencyLevel ConsistencyLevel;

        public KafkaProducerBrokerTopic([NotNull]string topicName, [NotNull] KafkaProducerSettings settings)
        {
            TopicName = topicName;
            Settings = settings;
            ConsistencyLevel = settings.ConsistencyLevel;
            Partitions = new ConcurrentDictionary<int, KafkaProducerBrokerPartition>();
        }
    }
}
