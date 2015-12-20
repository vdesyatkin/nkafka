using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceRequestTopicPartition
    {
        /// <summary>
        /// The partition that data is being published to.
        /// </summary>
        public int PartitionId { get; private set; }

        public KafkaCodecType Codec { get; private set; }

        public IReadOnlyList<KafkaMessage> Messages { get; private set; }

        public KafkaProduceRequestTopicPartition(int partitionId, KafkaCodecType codec, IReadOnlyList<KafkaMessage> messages)
        {
            PartitionId = partitionId;
            Codec = codec;
            Messages = messages;            
        }
    }
}
