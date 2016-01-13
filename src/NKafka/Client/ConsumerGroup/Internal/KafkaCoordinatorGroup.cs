using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaClientTopic> Topics;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        public KafkaCoordinatorGroupStatus Status;

        public int GroupGenerationId;
        public string GroupProtocolName;
        public short GroupProtocolVersion;
        public string MemberId;
        public bool MemberIsLeader;
        [CanBeNull] public IReadOnlyList<KafkaCoordinatorGroupMember> GroupMembers;
        [CanBeNull] public IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;

        [CanBeNull] public IReadOnlyList<string> AdditionalTopicNames;
        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicPartitions;

        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyList<int>> MemberAssignment;
        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyDictionary<int, long>> TopicPartitionOffsets;

        public KafkaCoordinatorGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Topics = topics;
            TopicPartitions = new Dictionary<string, IReadOnlyList<int>>();
            Settings = settings;
        }                       
    }
}
