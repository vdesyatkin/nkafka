using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// Topic metadata.
    /// </summary>
    [PublicAPI]
    public sealed class KafkaTopicMetadataResponseTopic
    {
        /// <summary>
        /// Error code.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// Topic name.
        /// </summary>
        public readonly string TopicName;

        /// <summary>
        /// Indicates if the topic is considered a Kafka internal topic.
        /// </summary>
        public readonly bool? IsInternalTopic;

        /// <summary>
        /// Topic partitions metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseTopicPartition> Partitions;

        /// <param name="errorCode">Error code.</param>
        /// <param name="topicName">Topic name.</param>
        /// <param name="isInternalTopic">Indicates if the topic is considered a Kafka internal topic.</param>
        /// <param name="partitions">Topic partitions metadata.</param>
        public KafkaTopicMetadataResponseTopic(
            KafkaResponseErrorCode errorCode,
            string topicName,
            bool? isInternalTopic,
            IReadOnlyList<KafkaTopicMetadataResponseTopicPartition> partitions)
        {
            ErrorCode = errorCode;
            TopicName = topicName;
            IsInternalTopic = isInternalTopic;            
            Partitions = partitions;
        }
    }
}
