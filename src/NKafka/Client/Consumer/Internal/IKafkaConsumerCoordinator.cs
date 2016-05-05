using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerCoordinator
    {
        [NotNull] KafkaConsumerGroupInfo GetDiagnosticsInfo();
        [CanBeNull] IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets([NotNull] string topicName);
    }
}
