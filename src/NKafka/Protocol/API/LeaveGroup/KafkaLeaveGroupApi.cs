using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.LeaveGroup
{
    [PublicAPI]
    internal sealed class KafkaLeaveGroupApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaLeaveGroupRequest);

        #region LeaveGroupRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteLeaveGroupRequest(writer, (KafkaLeaveGroupRequest)request);
        }

        private static void WriteLeaveGroupRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaLeaveGroupRequest request)
        {
            writer.WriteString(request.GroupId);            
            writer.WriteString(request.MemberId);
        }

        #endregion LeaveGroupRequest

        #region LeaveGroupResponse

        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadLeaveGroupResponse(reader);
        }

        private static KafkaLeaveGroupResponse ReadLeaveGroupResponse([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            return new KafkaLeaveGroupResponse(errorCode);
        }

        #endregion LeaveGroupResponse
    }
}
