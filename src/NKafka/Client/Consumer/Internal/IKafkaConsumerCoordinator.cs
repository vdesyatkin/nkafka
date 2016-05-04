using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerCoordinator
    {
        [NotNull] string GroupName { get; }
        [CanBeNull] IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets([NotNull] string topicName);
    }
}
