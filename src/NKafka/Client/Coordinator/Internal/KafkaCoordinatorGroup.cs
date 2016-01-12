using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;

namespace NKafka.Client.Coordinator.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull, ItemNotNull]
        public IReadOnlyList<KafkaClientTopic> Topics { get; private set; }

        public KafkaCoordinatorGroupStatus Status;

        public string MemberId;

        public KafkaCoordinatorGroup()
        {
            Topics = new KafkaClientTopic[0];
        }

        public void SetTopics([NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics)
        {
            Topics = topics;
        }
    }
}
