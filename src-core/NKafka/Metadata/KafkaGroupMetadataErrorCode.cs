using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public enum KafkaGroupMetadataErrorCode 
    {
        UnknownError = 0,
        CoordinatorNotAvailable = 15,
        GroupAuthorizationFailed = 30
    }
}
