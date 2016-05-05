using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionInfo
    {
        [NotNull]
        public readonly string GroupName;        

        public readonly bool IsReady;

        public readonly KafkaConsumerGroupStatus Status;

        public readonly KafkaConsumerGroupErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtcUtc;

        [CanBeNull] public readonly KafkaConsumerGroupMemberInfo MemberInfo;

        [CanBeNull] public readonly KafkaConsumerGroupProtocolInfo ProtocolInfo;

        [CanBeNull] public readonly KafkaConsumerGroupOffsetsInfo OffsetsInfo;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupSessionInfo([NotNull] string groupName, bool isReady,
            KafkaConsumerGroupStatus status,
            KafkaConsumerGroupErrorCode? error, DateTime errorTimestampUtc,
            [CanBeNull]KafkaConsumerGroupMemberInfo memberInfo,
            [CanBeNull] KafkaConsumerGroupProtocolInfo protocolInfo,
            [CanBeNull]KafkaConsumerGroupOffsetsInfo offsetsInfo,
            DateTime timestampUtc
            )
        {
            GroupName = groupName;            
            IsReady = isReady;
            Status = status;
            Error = error;
            ErrorTimestampUtcUtc = errorTimestampUtc;            
            MemberInfo = memberInfo;
            ProtocolInfo = protocolInfo;
            OffsetsInfo = offsetsInfo;
            TimestampUtc = timestampUtc;
        }
    }
}
