using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupInfo
    {
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaConsumerGroupStatus Status;

        public readonly KafkaConsumerGroupErrorCode? Error;

        public readonly KafkaClientBrokerInfo CoordinatorBrokerInfo;

        public readonly KafkaConsumerGroupProtocolInfo ProtocolInfo;

        public readonly KafkaConsumerGroupMemberInfo MemberInfo;

        public readonly IReadOnlyList<KafkaConsumerGroupTopicInfo> Topics;

        public KafkaConsumerGroupInfo(string groupName, DateTime timestampUtc, bool isReady, KafkaConsumerGroupStatus status, KafkaConsumerGroupErrorCode? error,
            KafkaClientBrokerInfo coordinatorBrokerInfo,
            KafkaConsumerGroupProtocolInfo protocolInfo, KafkaConsumerGroupMemberInfo memberInfo, 
            IReadOnlyList<KafkaConsumerGroupTopicInfo> topics)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Status = status;
            Error = error;
            CoordinatorBrokerInfo = coordinatorBrokerInfo;
            ProtocolInfo = protocolInfo;
            MemberInfo = memberInfo;
            Topics = topics;
        }
    }
}
