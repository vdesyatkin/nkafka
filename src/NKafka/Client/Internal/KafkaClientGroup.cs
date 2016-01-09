using JetBrains.Annotations;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {
        [NotNull] public readonly string GroupName;

        public KafkaClientGroupStatus Status;

        public KafkaClientGroup([NotNull] string groupName)
        {
            GroupName = groupName;
        }
    }
}
