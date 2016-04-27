using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    internal sealed class KafkaFetchResponseTopicPartition
    {
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// Error code.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
        /// </summary>
        public readonly long HighwaterMarkOffset;

        /// <summary>
        /// The message data fetched from this partition.
        /// </summary>
        public readonly IReadOnlyList<KafkaMessageAndOffset> Messages;        

        public KafkaFetchResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode, long highwaterMarkOffset, IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            HighwaterMarkOffset = highwaterMarkOffset;
            Messages = messages;            
        }
    }
}
