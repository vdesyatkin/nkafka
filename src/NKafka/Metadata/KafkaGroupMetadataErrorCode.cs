using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public enum KafkaGroupMetadataErrorCode : byte
    {
        UnknownError = 0,
        CoordinatorNotAvailable = 15,
        AuthorizationFailed = 30
    }
}
