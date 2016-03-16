using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol
{
    internal interface IKafkaRequestApi
    {
        [NotNull] Type RequestType { get; }
        void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request);
        IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader);
    }
}
