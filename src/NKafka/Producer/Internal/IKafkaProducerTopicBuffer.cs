using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Producer.Internal
{
    internal interface IKafkaProducerTopicBuffer
    {
        void Flush([NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartition> partitons);
    }
}
