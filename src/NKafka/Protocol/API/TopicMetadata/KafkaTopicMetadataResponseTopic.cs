using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// Topic metadata.
    /// </summary>
    [PublicAPI]
    internal sealed class KafkaTopicMetadataResponseTopic
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
        /// Topic partitions metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseTopicPartition> Partitions;
        
        /// <param name="errorCode">Error code.</param>
        /// <param name="topicName">Topic name.</param>        
        /// <param name="partitions">Topic partitions metadata.</param>
        public KafkaTopicMetadataResponseTopic(
            KafkaResponseErrorCode errorCode,
            string topicName, 
            IReadOnlyList<KafkaTopicMetadataResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            ErrorCode = errorCode;
            Partitions = partitions;
        }
    }
}
