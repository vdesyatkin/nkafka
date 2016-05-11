using System;
using JetBrains.Annotations;

namespace NKafka.Protocol
{
    internal sealed class KafkaProtocolException : Exception
    {
        [PublicAPI]
        public readonly KafkaProtocolErrorCode Error;

        public KafkaProtocolException(KafkaProtocolErrorCode error)
        {
            Error = error;
        }
    }
}
