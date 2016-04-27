using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerCoordinator
    {
        [CanBeNull] IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets([NotNull] string topicName);
    }
}
