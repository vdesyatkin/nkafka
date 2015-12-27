using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitionId;

        [NotNull]
        private readonly IKafkaConsumerTopic _consumer;

        public bool NeedRearrange;

        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId, [NotNull] IKafkaConsumerTopic consumer)
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
