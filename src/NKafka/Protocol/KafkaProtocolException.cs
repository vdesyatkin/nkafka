using System;

namespace NKafka.Protocol
{
    internal sealed class KafkaProtocolException : Exception
    {
        public readonly KafkaProtocolErrorCode Error;

        public KafkaProtocolException(KafkaProtocolErrorCode error)
        {
            Error = error;
        }
    }
}
