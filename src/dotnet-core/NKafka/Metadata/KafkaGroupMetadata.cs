using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public sealed class KafkaGroupMetadata
    {
        [NotNull] public readonly string GroupName;

        public KafkaGroupMetadataErrorCode? Error;

        [CanBeNull] public readonly KafkaBrokerMetadata Coordinator;

        public KafkaGroupMetadata([NotNull] string groupName, KafkaGroupMetadataErrorCode? error, [CanBeNull] KafkaBrokerMetadata coordinator)
        {
            GroupName = groupName;
            Error = error;
            Coordinator = coordinator; 
        }
    }
}
