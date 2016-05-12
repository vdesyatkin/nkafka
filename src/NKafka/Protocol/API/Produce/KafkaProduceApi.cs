using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
    internal class KafkaProduceApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaProduceRequest);

        const byte MessageMagicByteV09 = 0;
        const byte MessageMagicByteV010 = 1;

        const byte MessageEmptyAttribute = 0;
        const byte MessageAttributeCodecMask = 0x7;
        const byte MessageGZipAttribute = MessageAttributeCodecMask & (byte)KafkaCodecType.CodecGzip;
        const byte MessageAttributeTimestampMask = 0x8;
        const byte MessageTimestampLogAppendAttribute = MessageAttributeTimestampMask & (byte)KafkaTimestampType.LogAppendTime;
        
        private readonly KafkaRequestVersion _requestVersion;

        public KafkaProduceApi(KafkaRequestVersion requestVersion)
        {            
            _requestVersion = requestVersion;
        }

        #region ProduceRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteProduceRequest(writer, (KafkaProduceRequest)request);
        }

        private void WriteProduceRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceRequest request)
        {
            writer.WriteInt16((short)request.RequiredAcks);
            writer.WriteInt32((int)request.Timeout.TotalMilliseconds);
            writer.WriteCollection(request.Topics, WriteProduceRequestTopic);
        }

        private void WriteProduceRequestTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceRequestTopic topic)
        {            
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteProduceRequestTopicPartition);            
        }

        private void WriteProduceRequestTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaProduceRequestTopicPartition partition)
        {            
            var magicByte = _requestVersion >= KafkaRequestVersion.V2 ? MessageMagicByteV010 : MessageMagicByteV09;
            var useTimestamp = _requestVersion >= KafkaRequestVersion.V2;

            var defaultTimestampUtc = DateTime.UtcNow;

            writer.WriteInt32(partition.PartitionId);

            writer.BeginWriteSize();

            if (partition.Codec == KafkaCodecType.CodecGzip)
            {                
                // single gzip message
                writer.WriteInt64(0); //offset

                writer.BeginWriteSize();
                writer.BeginWriteCrc2();

                writer.WriteInt8(magicByte);
                writer.WriteInt8(MessageGZipAttribute);
                if (useTimestamp)
                {
                    writer.WriteTimestampUtc(defaultTimestampUtc); // (V010) todo??
                }
                writer.WriteByteArray(null); // key

                writer.BeginWriteGZipData(); //value
                if (partition.Messages != null)
                {                    
                    var nestedOffset = 0;
                    foreach (var message in partition.Messages)
                    {
                        if (message == null) continue;
                        writer.WriteInt64(nestedOffset);
                        nestedOffset++;

                        writer.BeginWriteSize();
                        writer.BeginWriteCrc2();

                        writer.WriteInt8(magicByte);
                        writer.WriteInt8(MessageEmptyAttribute);
                        if (useTimestamp)
                        {
                            writer.WriteTimestampUtc(message.TiemestampUtc ?? defaultTimestampUtc);
                        }
                        writer.WriteByteArray(message.Key);
                        writer.WriteByteArray(message.Data);

                        writer.EndWriteCrc2();
                        writer.EndWriteSize();
                    }                    
                }
                writer.EndWriteGZipData();

                writer.EndWriteCrc2();
                writer.EndWriteSize();
                // end of single gzip message
            }
            else
            {                
                // ordinary message set
                if (partition.Messages != null)
                {
                    foreach (var message in partition.Messages)
                    {
                        if (message == null) continue;
                        writer.WriteInt64(0); //offset

                        writer.BeginWriteSize();
                        writer.BeginWriteCrc2();

                        writer.WriteInt8(magicByte);
                        writer.WriteInt8(MessageEmptyAttribute);
                        if (useTimestamp)
                        {
                            writer.WriteTimestampUtc(defaultTimestampUtc);
                        }
                        writer.WriteByteArray(message.Key);
                        writer.WriteByteArray(message.Data);

                        writer.EndWriteCrc2();
                        writer.EndWriteSize();
                    }
                }
            }           

            writer.EndWriteSize();
        }
        
        #endregion ProduceRequest

        #region ProduceResponse
        
        [PublicAPI]
        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadProduceResponse(reader);
        }

        [NotNull]
        private KafkaProduceResponse ReadProduceResponse([NotNull] KafkaBinaryReader reader)
        {
            var topics = reader.ReadCollection(ReadProduceResponseTopic);
            var throttleTime = _requestVersion >= KafkaRequestVersion.V1 ? TimeSpan.FromMilliseconds(reader.ReadInt32()) : TimeSpan.Zero;
            return new KafkaProduceResponse(topics, throttleTime);
        }

        private KafkaProduceResponseTopic ReadProduceResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadProduceResponseTopicPartition);

            return new KafkaProduceResponseTopic(topicName, partitions);
        }

        private KafkaProduceResponseTopicPartition ReadProduceResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var errorCode = (KafkaResponseErrorCode) reader.ReadInt16();
            var offset = reader.ReadInt64();
            var timestampUtc = _requestVersion >= KafkaRequestVersion.V2 ? reader.ReadNulalbleTimestampUtc() : null;
            return new KafkaProduceResponseTopicPartition(partitionId, errorCode, offset, timestampUtc);
        }

        #endregion ProduceResponse        
    }
}