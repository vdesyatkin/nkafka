using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        public readonly string TopicName;

        public readonly int PartitionId;

        [NotNull] private readonly IKafkaConsumerTopic _consumer;

        [NotNull]
        public readonly KafkaBrokerMetadata BrokerMetadata;        

        public bool IsUnplugRequired;

        public KafkaConsumerBrokerPartitionStatus Status;

        public KafkaConsumerBrokerPartition(string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata, [NotNull] IKafkaConsumerTopic consumer)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            BrokerMetadata = brokerMetadata;
            _consumer = consumer;
        }

        public void Consume([NotNull] IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            try
            {
                _consumer.Consume(messages);
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
