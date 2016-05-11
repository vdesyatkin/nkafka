using System;
using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal sealed class KafkaConnectionException : Exception
    {
        public readonly KafkaConnectionErrorCode ErrorCode;

        [CanBeNull] public readonly KafkaConnectionSocketErrorInfo SocketError;

        public KafkaConnectionException(KafkaConnectionErrorCode errorCode) : base(errorCode.ToString())
        {
            ErrorCode = errorCode;
            SocketError = null;
        }

        public KafkaConnectionException(KafkaConnectionErrorCode errorCode, [CanBeNull] Exception innerException,
            [CanBeNull] KafkaConnectionSocketErrorInfo socketError = null) : base(
                innerException != null ? $"[{errorCode}] {innerException.Message})" : errorCode.ToString(), 
                innerException)
        {
            ErrorCode = errorCode;
            SocketError = socketError;
        }
    }
}
