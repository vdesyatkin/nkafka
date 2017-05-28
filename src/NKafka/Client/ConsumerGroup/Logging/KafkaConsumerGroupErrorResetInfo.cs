using System;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupErrorResetInfo
    {
        public readonly KafkaConsumerGroupErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerGroupErrorResetInfo(KafkaConsumerGroupErrorCode errorCode, DateTime? errorTimestampUtc,
            [NotNull] IKafkaClientBroker broker)
        {
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
            Broker = broker;
        }
    }
}
