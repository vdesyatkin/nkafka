using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupAssignmentData
    {
        [NotNull]
        public readonly IReadOnlyDictionary<string, IReadOnlyList<int>> AssignedTopicPartitions;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupAssignmentData([NotNull] IReadOnlyDictionary<string, IReadOnlyList<int>> assignedTopicPartitions, DateTime timestampUtc)
        {
            AssignedTopicPartitions = assignedTopicPartitions;
            TimestampUtc = timestampUtc;
        }
    }
}
