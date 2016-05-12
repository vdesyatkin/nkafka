using System;
using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{    
    [PublicAPI]
    public sealed class KafkaBrokerErrorInfo
    {
        public readonly KafkaBrokerErrorCode ErrorCode;

        [CanBeNull]
        public readonly KafkaBrokerRequestInfo RequestInfo;

        [CanBeNull]
        public readonly Exception Exception;

        public KafkaBrokerErrorInfo(KafkaBrokerErrorCode errorCode,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo,
            [CanBeNull] Exception exception)
        {
            ErrorCode = errorCode;
            Exception = exception;
        }
    }
}
