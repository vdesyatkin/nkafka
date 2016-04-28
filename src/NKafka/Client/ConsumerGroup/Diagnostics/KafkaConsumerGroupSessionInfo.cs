using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionInfo
    {
        [NotNull]
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaConsumerGroupSessionStatus Status;

        public readonly KafkaConsumerGroupSessionErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtcUtc;

        [CanBeNull] public readonly KafkaConsumerGroupSessionMemberInfo MemberInfo;

        [CanBeNull] public readonly KafkaConsumerGroupSessionProtocolInfo ProtocolInfo;

        [CanBeNull] public readonly KafkaConsumerGroupSessionOffsetsInfo OffsetsInfo;

        

        public KafkaConsumerGroupSessionInfo([NotNull] string groupName, DateTime timestampUtc, bool isReady,
            KafkaConsumerGroupSessionStatus status,
            KafkaConsumerGroupSessionErrorCode? error, DateTime errorTimestampUtc,
            [CanBeNull]KafkaConsumerGroupSessionMemberInfo memberInfo,
            [CanBeNull] KafkaConsumerGroupSessionProtocolInfo protocolInfo,
            [CanBeNull]KafkaConsumerGroupSessionOffsetsInfo offsetsInfo
            )
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Status = status;
            Error = error;
            ErrorTimestampUtcUtc = errorTimestampUtc;            
            OffsetsInfo = offsetsInfo;
            ProtocolInfo = protocolInfo;
            MemberInfo = memberInfo;
        }
    }
}
