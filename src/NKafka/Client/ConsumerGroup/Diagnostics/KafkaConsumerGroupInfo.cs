using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Metadata;

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

        public readonly KafkaBrokerMetadata CoordinatorBrokerMetadata;

        public readonly KafkaConsumerGroupProtocolInfo ProtocolInfo;

        public readonly KafkaConsumerGroupMemberInfo MemberInfo;

        public readonly IReadOnlyList<KafkaConsumerGroupTopicInfo> Topics;

        public KafkaConsumerGroupInfo(string groupName, DateTime timestampUtc, bool isReady, KafkaConsumerGroupStatus status, KafkaConsumerGroupErrorCode? error,
            KafkaBrokerMetadata coordinatorBrokerMetadata,
            KafkaConsumerGroupProtocolInfo protocolInfo, KafkaConsumerGroupMemberInfo memberInfo, 
            IReadOnlyList<KafkaConsumerGroupTopicInfo> topics)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Status = status;
            Error = error;
            CoordinatorBrokerMetadata = coordinatorBrokerMetadata;
            ProtocolInfo = protocolInfo;
            MemberInfo = memberInfo;
            Topics = topics;
        }
    }
}
