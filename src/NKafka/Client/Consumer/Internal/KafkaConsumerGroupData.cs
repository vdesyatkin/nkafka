using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerGroupData
    {        
        [NotNull] public readonly IKafkaConsumerCoordinator GroupCoordinator;
     
        [CanBeNull] public readonly IKafkaConsumerCoordinator CatchUpGroupCoordinator;

        public KafkaConsumerGroupData([NotNull] IKafkaConsumerCoordinator groupCoordinator, [CanBeNull] IKafkaConsumerCoordinator catchUpGroupCoordinator)
        {
            GroupCoordinator = groupCoordinator;
            CatchUpGroupCoordinator = catchUpGroupCoordinator;
        }
    }
}
