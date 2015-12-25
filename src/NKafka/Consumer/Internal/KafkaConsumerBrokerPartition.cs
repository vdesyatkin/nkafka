using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Consumer.Internal
{
    public class KafkaConsumerBrokerPartition
    {
        public readonly string TopicName;

        public readonly int PartitionId;

        [NotNull] private readonly IKafkaConsumer _consumer;

        public KafkaConsumerBrokerPartition(string topicName, int partitionId, [NotNull] IKafkaConsumer consumer)
        {
            TopicName = topicName;
            PartitionId = partitionId;
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
