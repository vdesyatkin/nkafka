using System;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
    internal sealed class KafkaProduceResponseTopicPartition
    {
        /// <summary>
        /// The partition this response entry corresponds to.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// The error from this partition, if any. 
        /// Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a different host, 
        /// while others may have successfully accepted the produce request.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// The offset assigned to the first message in the message set appended to this partition.
        /// </summary>
        public readonly long Offset;

        /// <summary>
        /// If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. All the messages in the message set have the same timestamp.<br/>
        /// If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the produce request has been accepted by the broker if there is no error code returned.<br/>
        /// Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        /// </summary>
        public readonly DateTime? TimestampUtc;

        public KafkaProduceResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode, long offset, DateTime? timestampUtc)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            Offset = offset;
            TimestampUtc = timestampUtc;
        }
    }
}
