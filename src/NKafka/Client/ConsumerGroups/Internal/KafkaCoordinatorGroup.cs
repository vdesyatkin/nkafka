using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroups.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull, ItemNotNull]
        public IReadOnlyList<KafkaClientTopic> Topics { get; private set; }

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        public KafkaCoordinatorGroupStatus Status;

        public string MemberId;

        public KafkaCoordinatorGroup([NotNull] KafkaConsumerGroupSettings settings)
        {
            Topics = new KafkaClientTopic[0];
            Settings = settings;
        }

        public void SetTopics([NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics)
        {
            Topics = topics;
        }
    }
}
