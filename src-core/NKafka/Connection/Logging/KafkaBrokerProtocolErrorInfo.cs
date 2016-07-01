using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Protocol;

namespace NKafka.Connection.Logging
{    
    [PublicAPI]
    public sealed class KafkaBrokerProtocolErrorInfo
    {
        public readonly KafkaBrokerErrorCode ErrorCode;

        public readonly string ErrorDescription;

        public readonly KafkaProtocolErrorCode ProtocolError;

        [CanBeNull] public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerProtocolErrorInfo(KafkaBrokerErrorCode errorCode,
            string errorDescription,
            KafkaProtocolErrorCode protocolError,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo,
            [CanBeNull] Exception exception)
        {
            ErrorCode = errorCode;
            ErrorDescription = errorDescription;
            ProtocolError = protocolError;
            RequestInfo = requestInfo;
            Exception = exception;
        }
    }
}
