using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.JoinGroup
{
    [PublicAPI]
    internal sealed class KafkaJoinGroupApi : IKafkaRequestApi
    {       
        #region JoinGroupRequest

        private const string DefaultProtocolType = "consumer";
        private const string UnknownMember = "";

        private readonly KafkaRequestVersion _requestVersion;

        public KafkaJoinGroupApi(KafkaRequestVersion requestVersion)
        {
            _requestVersion = requestVersion;
        }

        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteJoinGroupRequest(writer, (KafkaJoinGroupRequest)request);
        }

        private void WriteJoinGroupRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaJoinGroupRequest request)
        {
            writer.WriteString(request.GroupName);
            writer.WriteInt32((int)request.SessionTimeout.TotalMilliseconds);
            if (_requestVersion >= KafkaRequestVersion.V1)
            {
                writer.WriteInt32((int)request.RebalanceTimeout.TotalMilliseconds);
            }
            writer.WriteString(request.MemberId ?? UnknownMember);
            writer.WriteString(DefaultProtocolType);
            writer.WriteCollection(request.Protocols, WriteJoinGroupRequestProtocol);
        }

        private static void WriteJoinGroupRequestProtocol([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaJoinGroupRequestProtocol protocol)
        {
            writer.WriteString(protocol.ProtocolName);

            writer.BeginWriteSize();
            writer.WriteInt16(protocol.ProtocolVersion);            
            writer.WriteCollection(protocol.TopicNames, writer.WriteString);
            writer.WriteByteArray(protocol.CustomData);
            writer.WriteCollection(protocol.AssignmentStrategies, writer.WriteString);
            writer.EndWriteSize();
        }      

        #endregion JoinGroupRequest

        #region JoinGroupResponse
        
        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadJoinGroupResponse(reader);
        }

        [NotNull]
        private static KafkaJoinGroupResponse ReadJoinGroupResponse([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var groupGenerationId = reader.ReadInt32();
            var groupProtocol = reader.ReadString();
            var groupLeaderId = reader.ReadString();
            var memberId = reader.ReadString();
            var members = reader.ReadCollection(ReadJoinGroupResponseMember);
            return new KafkaJoinGroupResponse(errorCode, groupGenerationId, groupProtocol, groupLeaderId, memberId, members);
        }

        private static KafkaJoinGroupResponseMember ReadJoinGroupResponseMember([NotNull] KafkaBinaryReader reader)
        {
            var memberId = reader.ReadString();

            var requiredSize = reader.BeginReadSize();
            if (requiredSize <= 0) return null;

            var protocolVersion = reader.ReadInt16();
            var topicNames = reader.ReadCollection(reader.ReadString);
            var customData = reader.ReadByteArray();
            var assignmentStrategies = reader.ReadCollection(reader.ReadString);

            var actualSize = reader.EndReadSize();
            if (actualSize != requiredSize)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedDataSize);
            }
                        
            return new KafkaJoinGroupResponseMember(memberId, protocolVersion, topicNames, assignmentStrategies, customData);
        }        

        #endregion JoinGroupResponse
    }
}
