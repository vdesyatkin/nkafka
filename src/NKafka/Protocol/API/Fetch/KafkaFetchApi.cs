using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    internal static class KafkaFetchApi
    {
        const byte MessageAttributeCodeMask = 3;
        const byte MessageGZipAttribute = 0x00 | (MessageAttributeCodeMask & (byte)KafkaCodecType.CodecGzip);

        #region FetchRequest        
        
        public static void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request)
        {
            WriteFetchRequest(writer, (KafkaFetchRequest)request);
        }

        private static void WriteFetchRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaFetchRequest request)
        {
            writer.WriteNullableInt32(request.ReplicaId);
            writer.WriteInt32((int)(request.MaxWaitTime.TotalMilliseconds));
            writer.WriteInt32(request.MinBytes);
            writer.WriteCollection(request.Topics, WriteFetchRequestTopic);
        }

        private static void WriteFetchRequestTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaFetchRequestTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteFetchRequestTopicPartition);
        }

        private static void WriteFetchRequestTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaFetchRequestTopicPartition partition)
        {
            writer.WriteInt32(partition.PartitionId);
            writer.WriteInt64(partition.FetchOffset);
            writer.WriteInt32(partition.MaxBytes);
        }

        #endregion FetchRequest

        #region FetchResponse
        
        public static IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader)
        {
            return ReadFetchResponse(reader);
        }

        private static KafkaFetchResponse ReadFetchResponse([NotNull] KafkaBinaryReader reader)
        {
            var topics = reader.ReadCollection(ReadFetchResponseTopic);
            return new KafkaFetchResponse(topics);
        }

        private static KafkaFetchResponseTopic ReadFetchResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadFetchResponseTopicPartition);

            return new KafkaFetchResponseTopic(topicName, partitions);
        }

        private static KafkaFetchResponseTopicPartition ReadFetchResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            // ReSharper disable UnusedVariable

            var partitionId = reader.ReadInt32();
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var highwaterMarkOffset = reader.ReadInt64();

            var messages = new List<KafkaMessageAndOffset>();
            reader.BeginReadSize();
            
            while (!reader.EndReadSize())
            {
                var offset = reader.ReadInt64();

                reader.BeginReadSize();
                reader.BeginReadCrc32();
                
                var magicNumber = reader.ReadInt8();
                var attribute = reader.ReadInt8();
                var key = reader.ReadByteArray();

                if (attribute == MessageGZipAttribute)
                {
                    // gzip message
                    reader.BeginReadGZipData();
                    while (!reader.EndReadGZipData())
                    {
                        // nested message set
                        var nestedOffset = reader.ReadInt64();

                        reader.BeginReadSize();
                        reader.BeginReadCrc32();

                        var nestedKey = reader.ReadByteArray();
                        var nestedValue = reader.ReadByteArray();

                        var nestedIsCrcValid = reader.EndReadCrc32(); //todo (E005) invalid CRC
                        var nestedIsSizeValid = reader.EndReadSize(); //todo (E005) invalid Size

                        if (!nestedIsCrcValid || !nestedIsSizeValid) continue;
                        var nestedMessage = new KafkaMessageAndOffset(nestedOffset, nestedKey, nestedValue);
                        messages.Add(nestedMessage);
                    }
                }
                else
                {
                    //ordinary message
                    var value = reader.ReadByteArray();

                    var isCrcValid = reader.EndReadCrc32(); //todo (E005) invalid CRC
                    var isSizeValid = reader.EndReadSize(); //todo (E005) invalid Size

                    if (!isCrcValid || !isSizeValid) continue;
                    var message = new KafkaMessageAndOffset(offset, key, value);
                    messages.Add(message);
                }
            }

            return new KafkaFetchResponseTopicPartition(partitionId, errorCode, highwaterMarkOffset, messages);
            // ReSharper enable UnusedVariable
        }

        #endregion FetchResponse
    }
}
