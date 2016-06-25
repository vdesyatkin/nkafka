using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Connection.Logging;

namespace NKafka.Connection
{
    [PublicAPI]
    internal sealed class KafkaConnectionException : Exception
    {
        [NotNull] public readonly KafkaConnectionErrorInfo ErrorInfo;        

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
