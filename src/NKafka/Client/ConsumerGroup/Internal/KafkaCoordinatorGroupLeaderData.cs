using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupLeaderData
    {
        [CanBeNull] public readonly string AssignmentStrategyName;
        [NotNull] public readonly IReadOnlyList<KafkaCoordinatorGroupMemberData> GroupMembers;
        [NotNull] public readonly IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMemberData>> TopicMembers;
        [NotNull] public readonly IReadOnlyList<string> AdditionalTopicNames;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupLeaderData([CanBeNull] string assignmentStrategyName, 
            [NotNull] IReadOnlyList<KafkaCoordinatorGroupMemberData> groupMembers,
            [NotNull] IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMemberData>> topicMembers,
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
