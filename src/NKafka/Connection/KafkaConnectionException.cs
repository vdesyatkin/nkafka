using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;

namespace NKafka.Connection
{
    [PublicAPI]
    internal sealed class KafkaConnectionException : Exception
    {
        [NotNull] public readonly KafkaConnectionErrorInfo ErrorInfo;

        [CanBeNull] public readonly KafkaConnectionSocketErrorInfo SocketError;

        public KafkaConnectionException(KafkaConnectionErrorCode errorCode) : base(errorCode.ToString())
        {
            ErrorInfo = new KafkaConnectionErrorInfo(errorCode, null);            
        }

        public KafkaConnectionException(KafkaConnectionErrorCode errorCode, [CanBeNull] Exception innerException,
            [CanBeNull] KafkaConnectionSocketErrorInfo socketError = null) : base(
                innerException != null ? $"[{errorCode}] {innerException.Message})" : errorCode.ToString(), 
                innerException)
        {
            ErrorInfo = new KafkaConnectionErrorInfo(errorCode, socketError);            
        }
    }
}
