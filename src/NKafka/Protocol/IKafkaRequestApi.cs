using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol
{
    internal interface IKafkaRequestApi
    {        
        void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request);
        [NotNull] IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader);
    }
}
