using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Producer.Internal
{
    internal class KafkaProducerTopic
    {     
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly IKafkaProducerTopicBuffer Buffer;

        [NotNull] public IReadOnlyList<KafkaProducerTopicPartition> Partitions;

        public KafkaProducerTopicStatus Status;

        public KafkaProducerTopic([NotNull] string topicName, [NotNull] IKafkaProducerTopicBuffer buffer)
        {
            TopicName = topicName;
            Buffer = buffer;
            Status = KafkaProducerTopicStatus.NotInitialized;
            Partitions = new KafkaProducerTopicPartition[0];
        }

        public void Flush()
        {
            Buffer.Flush(Partitions);
        }
    }  
}
