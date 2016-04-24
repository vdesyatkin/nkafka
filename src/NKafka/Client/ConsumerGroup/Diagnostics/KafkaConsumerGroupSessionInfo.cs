using System;
using System.Collections.Generic;
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

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupSessionTopicInfo> Topics;

        [CanBeNull]
        public readonly KafkaConsumerGroupSessionProtocolInfo ProtocolInfo;

        [CanBeNull]
        public readonly KafkaConsumerGroupSessionMemberInfo MemberInfo;

        public KafkaConsumerGroupSessionInfo([NotNull] string groupName, DateTime timestampUtc, bool isReady,
            KafkaConsumerGroupSessionStatus status, KafkaConsumerGroupSessionErrorCode? error,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerGroupSessionTopicInfo> topics, 
            [CanBeNull]KafkaConsumerGroupSessionProtocolInfo protocolInfo,
            [CanBeNull]KafkaConsumerGroupSessionMemberInfo memberInfo)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Status = status;
            Error = error;
            Topics = topics;
            ProtocolInfo = protocolInfo;
            MemberInfo = memberInfo;
        }
    }
}
