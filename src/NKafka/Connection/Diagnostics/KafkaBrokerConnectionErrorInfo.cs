using System;
using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaBrokerConnectionErrorInfo
    {        
        public readonly KafkaBrokerErrorCode ErrorCode;

        [NotNull] public readonly KafkaConnectionErrorInfo ConnectionError;

        [CanBeNull] public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerConnectionErrorInfo(KafkaBrokerErrorCode errorCode,            
            [NotNull] KafkaConnectionErrorInfo connectionError,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo,
            [CanBeNull] Exception exception)
        {            
            ErrorCode = errorCode;
            ConnectionError = connectionError;
            RequestInfo = requestInfo;
            Exception = exception;
        }
    }


}
