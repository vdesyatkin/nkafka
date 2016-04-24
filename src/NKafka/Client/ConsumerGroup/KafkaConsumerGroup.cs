using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup
{
    internal sealed class KafkaConsumerGroup : IKafkaConsumerGroup
    {
        public readonly string GroupName;

        public readonly KafkaConsumerGroupSettings Settings;

        string IKafkaConsumerGroup.GroupName => GroupName;        

        public KafkaConsumerGroup(string groupName, KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Settings = settings;
        }

        public KafkaConsumerGroupInfo GetDiagnosticsInfo()
        {
            //return new KafkaConsumerGroupInfo(GroupName, GroupTimestampUtc,
            //    false, //todo 
            //    KafkaConsumerGroupStatus.ToDo, //todo 
            //    KafkaConsumerGroupErrorCode.UnknownError, //todo
            //    new KafkaConsumerGroupProtocolInfo(GroupProtocolName, GroupProtocolVersion, GroupAssignmentStrategyName, null), //todo
            //    new KafkaConsumerGroupMemberInfo(GroupGenerationId, MemberId, MemberIsLeader),
            //    new KafkaConsumerGroupTopicInfo[0] //todo
            //    );

            return null;
        }
    }
}
