using System;
using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaBrokerErrorInfo
    {        
        public readonly KafkaBrokerErrorCode ErrorCode;

        [CanBeNull] public readonly KafkaConnectionErrorInfo ConnectionError;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerErrorInfo(KafkaBrokerErrorCode errorCode,
            [CanBeNull] KafkaConnectionErrorInfo connectionError, [CanBeNull] Exception exception)
        {            
            ErrorCode = errorCode;
            ConnectionError = connectionError;
            Exception = exception;
        }
    }


}
