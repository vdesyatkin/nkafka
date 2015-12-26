using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Produce
{
    internal static class KafkaProduceApi
    {        
        const byte MessageMagicNumber = 0;
        const byte MessageDefaultAttribute = 0;
        const byte MessageAttributeCodeMask = 3;
        const byte MessageGZipAttribute = 0x00 | (MessageAttributeCodeMask & (byte)KafkaCodecType.CodecGzip);

        #region ProduceRequest

        [PublicAPI]
        public static void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request)
        {
            WriteProduceRequest(writer, (KafkaProduceRequest)request);
        }

        private static void WriteProduceRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceRequest request)
        {
            writer.WriteInt16((short)request.RequiredAcks);
            writer.WriteInt32((int)request.Timeout.TotalMilliseconds);
            writer.WriteCollection(request.Topics, WriteProduceRequestTopic);
        }

        private static void WriteProduceRequestTopic(KafkaBinaryWriter writer, [NotNull] KafkaProduceRequestTopic topic)
        {            
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteProduceRequestTopicPartition);            
        }

        private static void WriteProduceRequestTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceRequestTopicPartition partition)
        {
            writer.WriteInt32(partition.PartitionId);

            writer.BeginWriteSize();

            if (partition.Codec == KafkaCodecType.CodecGzip)
            {                
                // single gzip message
                writer.WriteInt64(0); //offset
                writer.BeginWriteSize();
                writer.BeginWriteCrc2();
                writer.WriteInt8(MessageMagicNumber);
                writer.WriteInt8(MessageGZipAttribute);
                writer.WriteByteArray(null); // key

                writer.BeginWriteGZipData(); //value
                foreach (var message in partition.Messages)
                {
                    writer.WriteInt64(0); //offset

                    writer.BeginWriteSize();
                    writer.BeginWriteCrc2();

                    writer.WriteInt8(MessageMagicNumber);
                    writer.WriteInt8(MessageDefaultAttribute);
                    writer.WriteByteArray(message.Key);
                    writer.WriteByteArray(message.Data);

                    writer.EndWriteCrc2();
                    writer.EndWriteSize();
                }
                writer.EndWriteGZipData();

                writer.EndWriteCrc2();
                writer.EndWriteSize();
                // end of single gzip message
            }
            else
            {                
                // ordinary message set
                foreach (var message in partition.Messages)
                {
                    writer.WriteInt64(0); //offset

                    writer.BeginWriteSize();
                    writer.BeginWriteCrc2();

                    writer.WriteInt8(MessageMagicNumber);
                    writer.WriteInt8(MessageDefaultAttribute);
                    writer.WriteByteArray(message.Key);
                    writer.WriteByteArray(message.Data);

                    writer.EndWriteCrc2();
                    writer.EndWriteSize();
                }
            }           

            writer.EndWriteSize();
        }

        private static KafkaProduceRequest ReadProduceRequest([NotNull] KafkaBinaryReader reader)
        {
            var ackMode = (KafkaConsistencyLevel)reader.ReadInt16();
            var timeout = TimeSpan.FromMilliseconds(reader.ReadInt32());
            var topics = reader.ReadCollection(ReadProduceRequestTopic);
                        
            return new KafkaProduceRequest(ackMode, timeout, topics);
        }

        private static KafkaProduceRequestTopic ReadProduceRequestTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadProduceRequestTopicPartition);

            return new KafkaProduceRequestTopic(topicName, partitions);            
        }

        private static KafkaProduceRequestTopicPartition ReadProduceRequestTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();

            var messages = new List<KafkaMessage>();
            reader.BeginReadSize();

            var codec = KafkaCodecType.CodecNone;

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

                        var nestedMessage = new KafkaMessage(nestedKey, nestedValue);
                        messages.Add(nestedMessage);
                    }
                }
                else
                {
                    //ordinary message
                    var value = reader.ReadByteArray();

                    var isCrcValid = reader.EndReadCrc32(); //todo (E005) invalid CRC
                    var isSizeValid = reader.EndReadSize(); //todo (E005) invalid Size

                    var message = new KafkaMessage(key, value);
                    messages.Add(message);
                }                
            }

            return new KafkaProduceRequestTopicPartition(partitionId, codec, messages);
        }

        #endregion ProduceRequest

        #region ProduceResponse

        public static void WriteProduceResponse([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceResponse response)
        {
            writer.WriteCollection(response.Topics, WriteProduceResponseTopic);
        }

        private static void WriteProduceResponseTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceResponseTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteProduceResponseTopicPartition);
        }

        private static void WriteProduceResponseTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceResponseTopicPartition partition)
        {
            writer.WriteInt32(partition.PartitionId);
            writer.WriteInt16((short)partition.ErrorCode);
            writer.WriteInt64(partition.Offset);
        }

        [PublicAPI]
        public static IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader)
        {
            return ReadProduceResponse(reader);
        }

        private static KafkaProduceResponse ReadProduceResponse([NotNull] KafkaBinaryReader reader)
        {
            var topics = reader.ReadCollection(ReadProduceResponseTopic);
            return new KafkaProduceResponse(topics);
        }

        private static KafkaProduceResponseTopic ReadProduceResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadProduceResponseTopicPartition);

            return new KafkaProduceResponseTopic(topicName, partitions);
        }

        private static KafkaProduceResponseTopicPartition ReadProduceResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var errorCode = (KafkaResponseErrorCode) reader.ReadInt16();
            var offset = reader.ReadInt64();
            return new KafkaProduceResponseTopicPartition(partitionId, errorCode, offset);
        }

        #endregion ProduceResponse        
    }
}