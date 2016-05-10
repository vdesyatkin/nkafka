using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupApi : IKafkaRequestApi
    {
        public Type RequestType => typeof (KafkaSyncGroupRequest);

        #region SyncGroupRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteSyncGroupRequest(writer, (KafkaSyncGroupRequest)request);
        }

        private static void WriteSyncGroupRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaSyncGroupRequest request)
        {
            writer.WriteString(request.GroupName);
            writer.WriteInt32(request.GroupGenerationId);
            writer.WriteString(request.MemberId);            
            writer.WriteCollection(request.Members ?? new KafkaSyncGroupRequestMember[0], WriteSyncGroupRequestMember);
        }

        private static void WriteSyncGroupRequestMember([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaSyncGroupRequestMember member)
        {
            writer.WriteString(member.MemberId);

            writer.BeginWriteSize();
            writer.WriteInt16(member.ProtocolVersion);
            writer.WriteCollection(member.AssignedTopics, WriteSyncGroupRequestMemberTopic);
            writer.WriteByteArray(member.CustomData);            
            writer.EndWriteSize();
        }

        private static void WriteSyncGroupRequestMemberTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaSyncGroupRequestMemberTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.PartitionIds, writer.WriteInt32);
        }

        #endregion SyncGroupRequest

        #region SyncGroupResponse

        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadSyncGroupResponse(reader);
        }

        private static KafkaSyncGroupResponse ReadSyncGroupResponse([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();

            var requiredSize = reader.BeginReadSize();
            if (requiredSize <= 0) return new KafkaSyncGroupResponse(errorCode, 0, new KafkaSyncGroupResponseTopic[0], null);

            var protocolVersion = reader.ReadInt16();
            var topics = reader.ReadCollection(ReadSyncGroupResponseAssignmentTopic);
            var customData = reader.ReadByteArray();

            var actualSize = reader.EndReadSize();
            if (actualSize != requiredSize)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }

            return new KafkaSyncGroupResponse(errorCode, protocolVersion, topics, customData);
        }

        private static KafkaSyncGroupResponseTopic ReadSyncGroupResponseAssignmentTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitionIds = reader.ReadCollection(reader.ReadInt32);
            
            return new KafkaSyncGroupResponseTopic(topicName, partitionIds);
        }

        #endregion SyncGroupResponse
    }
}
