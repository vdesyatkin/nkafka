using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionInfo
    {
        [NotNull]
        public readonly string GroupName;        

        public readonly KafkaConsumerGroupStatus Status;

        public readonly KafkaConsumerGroupErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtc;

        [CanBeNull] public readonly KafkaConsumerGroupMemberInfo MemberInfo;

        [CanBeNull] public readonly KafkaConsumerGroupProtocolInfo ProtocolInfo;

        [CanBeNull] public readonly KafkaConsumerGroupOffsetsInfo OffsetsInfo;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupSessionInfo([NotNull] string groupName,
            KafkaConsumerGroupStatus status,
            KafkaConsumerGroupErrorCode? error, DateTime errorTimestampUtc,
            [CanBeNull]KafkaConsumerGroupMemberInfo memberInfo,
            [CanBeNull] KafkaConsumerGroupProtocolInfo protocolInfo,
            [CanBeNull]KafkaConsumerGroupOffsetsInfo offsetsInfo,
            DateTime timestampUtc
            )
        {
            GroupName = groupName;            
            Status = status;
            Error = error;
            ErrorTimestampUtc = errorTimestampUtc;            
            MemberInfo = memberInfo;
            ProtocolInfo = protocolInfo;
            OffsetsInfo = offsetsInfo;
            TimestampUtc = timestampUtc;
        }
    }
}
