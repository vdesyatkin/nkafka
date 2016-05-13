using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;

namespace NKafka.Connection.Logging
{
    [PublicAPI]
    public sealed class KafkaBrokerConnectionErrorInfo
    {        
        public readonly KafkaBrokerErrorCode ErrorCode;

        public readonly string ErrorDescription;

        [NotNull] public readonly KafkaConnectionErrorInfo ConnectionError;

        [CanBeNull] public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerConnectionErrorInfo(KafkaBrokerErrorCode errorCode,
            string errorDescription,
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
