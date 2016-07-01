using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;

namespace NKafka.Connection.Logging
{
    [PublicAPI]
    public sealed class KafkaBrokerTransportErrorInfo
    {        
        public readonly KafkaBrokerErrorCode ErrorCode;

        public readonly string ErrorDescription;

        [NotNull] public readonly KafkaConnectionErrorInfo ConnectionError;

        [CanBeNull] public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerTransportErrorInfo(KafkaBrokerErrorCode errorCode,
            string errorDescription,
            [NotNull] KafkaConnectionErrorInfo connectionError,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo,
            [CanBeNull] Exception exception)
        {            
            ErrorCode = errorCode;
            ErrorDescription = errorDescription;
            ConnectionError = connectionError;
            RequestInfo = requestInfo;
            Exception = exception;
        }
    }


}
