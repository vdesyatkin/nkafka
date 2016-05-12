using System;
using JetBrains.Annotations;

namespace NKafka.Protocol
{
    internal sealed class KafkaProtocolException : Exception
    {
        [PublicAPI]
        public readonly KafkaProtocolErrorCode ErrorCode;

        public KafkaProtocolException(KafkaProtocolErrorCode errorCode) : base(errorCode.ToString())
        {
            ErrorCode = errorCode;            
        }

        public KafkaProtocolException(KafkaProtocolErrorCode errorCode, [CanBeNull] Exception innerException) : base(
                innerException != null ? $"[{errorCode}] {innerException.Message})" : errorCode.ToString(), 
                innerException)            
        {
            ErrorCode = errorCode;
        }
    }
}
