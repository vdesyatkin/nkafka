using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;

namespace NKafka.Connection.Logging
{
    [PublicAPI]
    public sealed class KafkaConnectionErrorInfo
    {
        public readonly KafkaConnectionErrorCode ErrorCode;

        [CanBeNull] public readonly KafkaConnectionSocketErrorInfo SockerError;

        public KafkaConnectionErrorInfo(KafkaConnectionErrorCode errorCode, KafkaConnectionSocketErrorInfo sockerError)
        {
            ErrorCode = errorCode;
            SockerError = sockerError;
        }
    }
}
