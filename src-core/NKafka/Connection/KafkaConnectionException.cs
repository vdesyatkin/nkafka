using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Connection.Logging;

namespace NKafka.Connection
{
    [PublicAPI]
    internal sealed class KafkaConnectionException : Exception
    {
        [NotNull] public readonly KafkaConnection Connection;

        [NotNull] public readonly KafkaConnectionErrorInfo ErrorInfo;        

        public KafkaConnectionException([NotNull] KafkaConnection connection, KafkaConnectionErrorCode errorCode) 
            : base(errorCode.ToString())
        {
            Connection = connection;
            ErrorInfo = new KafkaConnectionErrorInfo(errorCode, null);
        }

        public KafkaConnectionException([NotNull] KafkaConnection connection, KafkaConnectionErrorCode errorCode, 
            [CanBeNull] Exception innerException,
            [CanBeNull] KafkaConnectionSocketErrorInfo socketError = null) 
            : base(innerException != null ? $"[{errorCode}] {innerException.Message})" : errorCode.ToString(), innerException)
        {
            Connection = connection;
            ErrorInfo = new KafkaConnectionErrorInfo(errorCode, socketError);            
        }
    }
}
