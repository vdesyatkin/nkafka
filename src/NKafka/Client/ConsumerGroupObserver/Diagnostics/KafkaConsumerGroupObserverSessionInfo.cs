using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverSessionInfo
    {
        [NotNull]
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaConsumerGroupObserverStatus Status;

        public readonly KafkaConsumerGroupObserverErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtcUtc;

        [CanBeNull]
        public readonly KafkaConsumerGroupObserverOffsetsInfo OffsetsInfo;

        public KafkaConsumerGroupObserverSessionInfo([NotNull] string groupName, DateTime timestampUtc, bool isReady,
            KafkaConsumerGroupObserverStatus status,
            KafkaConsumerGroupObserverErrorCode? error, DateTime errorTimestampUtc,            
            [CanBeNull]KafkaConsumerGroupObserverOffsetsInfo offsetsInfo
            )
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Status = status;
            Error = error;
            ErrorTimestampUtcUtc = errorTimestampUtc;
            OffsetsInfo = offsetsInfo;            
        }
    }
}
