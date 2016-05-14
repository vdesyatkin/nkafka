using System;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupErrorResetInfo
    {
        public readonly KafkaConsumerGroupErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        public KafkaConsumerGroupErrorResetInfo(KafkaConsumerGroupErrorCode errorCode, DateTime? errorTimestampUtc)
        {            
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
        }
    }
}
