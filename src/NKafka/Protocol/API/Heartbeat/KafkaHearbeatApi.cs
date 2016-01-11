﻿using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Heartbeat
{
    [PublicAPI]
    internal sealed class KafkaHearbeatApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaHeartbeatRequest);

        #region HeartbeatRequest

        private const string DefaultProtocolType = "consumer";
        private const string UnknownMember = "";

        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteHeartbeatRequest(writer, (KafkaHeartbeatRequest)request);
        }

        private static void WriteHeartbeatRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaHeartbeatRequest request)
        {
            writer.WriteString(request.GroupId);
            writer.WriteInt32(request.GroupGenerationId);
            writer.WriteString(request.MemberId);            
        }        

        #endregion HeartbeatRequest

        #region HeartbeatResponse

        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadHeartbeatResponse(reader);
        }

        private static KafkaHeartbeatResponse ReadHeartbeatResponse([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();            
            return new KafkaHeartbeatResponse(errorCode);
        }

        #endregion HeartbeatResponse
    }
}
