using System;
using JetBrains.Annotations;
using NKafka.Protocol;

namespace NKafka.Connection.Diagnostics
{    
    [PublicAPI]
    public sealed class KafkaBrokerProtocolErrorInfo
    {
        public readonly KafkaBrokerErrorCode ErrorCode;
                
        public readonly KafkaProtocolErrorCode ProtocolError;

        [CanBeNull] public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull] public readonly Exception Exception;

        public KafkaBrokerProtocolErrorInfo(KafkaBrokerErrorCode errorCode, 
            KafkaProtocolErrorCode protocolError,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo,
            [CanBeNull] Exception exception)
        {
            ErrorCode = errorCode;
            ProtocolError = protocolError;
            RequestInfo = requestInfo;
            Exception = exception;
        }
    }
}
