using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerPartitioner
    {
        int GetPartition([NotNull] KafkaMessage message, [NotNull] IReadOnlyList<int> partitions);
    }

    [PublicAPI]
    public interface IKafkaProducerPartitioner<TKey, TData>
    {
        int GetPartition([NotNull] KafkaMessage<TKey, TData> message, [NotNull] IReadOnlyList<int> partitions);
    }
}
