using System;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupAssignmentErrorInfo
    {
        [NotNull] public readonly KafkaConsumerAssignmentRequest AssignmentRequest;

        [CanBeNull] public readonly Exception Exception;

        public KafkaConsumerGroupAssignmentErrorInfo([NotNull] KafkaConsumerAssignmentRequest assignmentRequest, [CanBeNull] Exception exception)
        {
            AssignmentRequest = assignmentRequest;
            Exception = exception;
        }
    }
}
