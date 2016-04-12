using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public sealed class KafkaGroupMetadata
    {
        [NotNull] public readonly string GroupName;

        [NotNull] public readonly KafkaBrokerMetadata Coordinator;

        public KafkaGroupMetadata([NotNull] string groupName, [NotNull] KafkaBrokerMetadata coordinator)
        {
            GroupName = groupName;
            Coordinator = coordinator;
        }
    }
}
