using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupLeader
    {
        [CanBeNull] public readonly string AssignmentStrategyName;
        [NotNull] public readonly IReadOnlyList<KafkaCoordinatorGroupMember> GroupMembers;
        [NotNull] public readonly IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;
        [NotNull] public readonly IReadOnlyList<string> AdditionalTopicNames;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupLeader([CanBeNull] string assignmentStrategyName, 
            [NotNull] IReadOnlyList<KafkaCoordinatorGroupMember> groupMembers,
            [NotNull] IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> topicMembers,
            [NotNull] IReadOnlyList<string> additionalTopicNames,
            DateTime timestampUtc)
        {
            AssignmentStrategyName = assignmentStrategyName;
            GroupMembers = groupMembers;
            TopicMembers = topicMembers;
            AdditionalTopicNames = additionalTopicNames;
            TimestampUtc = timestampUtc;
        }
    }
}
