using System;
using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{    
    [PublicAPI]
    public sealed class KafkaBrokerInternalErrorInfo
    {
        public readonly KafkaBrokerErrorCode ErrorCode;       

        [CanBeNull]
        public readonly Exception Exception;

        public KafkaBrokerInternalErrorInfo(KafkaBrokerErrorCode errorCode, [CanBeNull] Exception exception)
        {
            ErrorCode = errorCode;            
            Exception = exception;
        }
    }
}
