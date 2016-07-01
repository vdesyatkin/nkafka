using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
    public sealed class KafkaProduceRequestTopicPartition
    {
        /// <summary>
        /// The partition that data is being published to.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// Kafka supports compressing messages for additional efficiency, however this is more complex than just compressing a raw message. 
        /// Because individual messages may not have sufficient redundancy to enable good compression ratios, compressed messages must be sent in special batches 
        /// (although you may use a batch of one if you truly wish to compress a message on its own). The messages to be sent are wrapped (uncompressed) 
        /// in a MessageSet structure, which is then compressed and stored in the Value field of a single "Message" with the appropriate compression codec set. 
        /// The receiving system parses the actual MessageSet from the decompressed value. 
        /// The outer MessageSet should contain only one compressed "Message" (see KAFKA-1718 for details).
        /// </summary>
        public readonly KafkaCodecType Codec;

        /// <summary>
        /// The message set.
        /// </summary>
        public readonly IReadOnlyList<KafkaMessage> Messages;

        public KafkaProduceRequestTopicPartition(int partitionId, KafkaCodecType codec, IReadOnlyList<KafkaMessage> messages)
        {
            PartitionId = partitionId;
            Codec = codec;
            Messages = messages;            
        }
    }
}
