using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
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
