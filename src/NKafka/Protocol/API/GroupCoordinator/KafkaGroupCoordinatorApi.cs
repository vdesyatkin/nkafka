using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.GroupCoordinator
{
    [PublicAPI]
    internal class KafkaGroupCoordinatorApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaGroupCoordinatorRequest);

        #region GroupCoordinatorRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteGroupCoordinatorRequest(writer, (KafkaGroupCoordinatorRequest)request);
        }

        private static void WriteGroupCoordinatorRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaGroupCoordinatorRequest request)
        {
            writer.WriteString(request.GroupId);            
        }

        #endregion GroupCoordinatorRequest

        #region GroupCoordinatorResponse
        
        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadGroupCoordinatorResponse(reader);
        }

        private static KafkaGroupCoordinatorResponse ReadGroupCoordinatorResponse([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var brokerId = reader.ReadInt32();
            var borkerHost = reader.ReadString();
            var brokerPort = reader.ReadInt32();
            return new KafkaGroupCoordinatorResponse(errorCode, brokerId, borkerHost, brokerPort);
        }        

        #endregion GroupCoordinatorResponse
    }
}
