using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public enum KafkaTopicMetadataErrorCode
    {
        UnknownError = 0,

        /// <summary>
        /// This request is for a topic that does not exist on this broker.
        /// </summary>
        UnknownTopic = 3,

        /// <summary>
        /// For a request which attempts to access an invalid topic (e.g. one which has an illegal name), <br/>
        /// or if an attempt is made to write to an internal topic (such as the consumer offsets topic).
        /// </summary>
        InvalidTopic = 17,

        /// <summary>
        /// Returned by the broker when the client is not authorized to access the requested topic.
        /// </summary>
        TopicAuthorizationFailed = 29
    }
}
