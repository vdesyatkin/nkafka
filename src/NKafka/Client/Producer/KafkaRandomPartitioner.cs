using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public class KafkaRandomPartitioner : IKafkaProducerPartitioner
    {
        [NotNull]
        private readonly Random _random = new Random();

        public int GetPartition(KafkaMessage message, IReadOnlyList<int> partitions)
        {
            return _random.Next(0, partitions.Count);
        }
    }

    [PublicAPI]
    public class KafkaRandomPartitioner<TKey, TData> : IKafkaProducerPartitioner<TKey, TData>
    {
        [NotNull]
        private readonly Random _random = new Random();

        public int GetPartition(KafkaMessage<TKey, TData> message, IReadOnlyList<int> partitions)
        {
            return _random.Next(0, partitions.Count);
        }
    }
}
