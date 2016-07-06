using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupOffsetsDataTopic
    {
        [NotNull] public readonly IReadOnlyDictionary<int, KafkaCoordinatorGroupOffsetsDataPartition> Partitions;
        [NotNull] public readonly IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> PartitionsReadOnly;

        public KafkaCoordinatorGroupOffsetsDataTopic([NotNull] IReadOnlyDictionary<int, KafkaCoordinatorGroupOffsetsDataPartition> partitions)
        {
            Partitions = partitions;
            
            var partitionsReadOnly = new Dictionary<int, IKafkaConsumerCoordinatorOffsetsData>(partitions.Count);
            foreach (var partition in partitions)
            {
                partitionsReadOnly[partition.Key] = partition.Value;
            }
            PartitionsReadOnly = partitionsReadOnly;
        }
    }
}
