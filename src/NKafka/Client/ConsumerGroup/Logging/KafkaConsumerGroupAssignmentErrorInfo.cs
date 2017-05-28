using System;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.ConsumerGroup.Assignment;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupAssignmentErrorInfo
    {
        [NotNull] public readonly KafkaConsumerAssignmentRequest AssignmentRequest;

        [CanBeNull] public readonly Exception Exception;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerGroupAssignmentErrorInfo([NotNull] KafkaConsumerAssignmentRequest assignmentRequest, [NotNull] IKafkaClientBroker broker, [CanBeNull] Exception exception)
        {
            AssignmentRequest = assignmentRequest;
            Broker = broker;
            Exception = exception;
        }
    }
}
