using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull, ItemNotNull]
        public IReadOnlyList<KafkaClientTopic> Topics { get; private set; }

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        public KafkaCoordinatorGroupStatus Status;

        public int GroupGenerationId;
        public string GroupProtocolName;
        public string MemberId;
        public bool IsLeader;
        [CanBeNull] public Dictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;

        [CanBeNull] public List<string> AdditionalTopicNames;
        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicPartitions;

        public KafkaCoordinatorGroup([NotNull] KafkaConsumerGroupSettings settings)
        {
            Topics = new KafkaClientTopic[0];
            TopicPartitions = new Dictionary<string, IReadOnlyList<int>>();
            Settings = settings;
        }

        public void SetTopics([NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics)
        {
            Topics = topics;
        }               
    }
}
