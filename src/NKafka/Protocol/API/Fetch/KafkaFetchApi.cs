using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    internal class KafkaFetchApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaFetchRequest);

        const byte MessageMagicByteV09 = 0;
        const byte MessageMagicByteV010 = 1;

        const byte MessageEmptyAttribute = 0;

        const byte MessageAttributeCodecMask = 0x7;
        const byte MessageCodecNoneAttribute = MessageAttributeCodecMask & (byte)KafkaCodecType.CodecNone;
        const byte MessageCodecGZipAttribute = MessageAttributeCodecMask & (byte)KafkaCodecType.CodecGzip;
        //todo (v10) snappy

        const byte MessageAttributeTimestampMask = 0x8;
        const byte MessageTimestampLogAppendTimeAttribute = MessageAttributeTimestampMask & (byte)KafkaTimestampType.LogAppendTime << 3;
        const byte MessageTimestampCreateTimeAttribute = MessageAttributeTimestampMask & (byte)KafkaTimestampType.CreateTime << 3;

        private readonly KafkaRequestVersion _requestVersion;

        public KafkaFetchApi(KafkaRequestVersion requestVersion)
        {            
            _requestVersion = requestVersion;
        }

        #region FetchRequest        

        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
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
        
        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadFetchResponse(reader);
        }

        private KafkaFetchResponse ReadFetchResponse([NotNull] KafkaBinaryReader reader)
        {
            var throttleTime = _requestVersion >= KafkaRequestVersion.V1 ? TimeSpan.FromMilliseconds(reader.ReadInt32()) : TimeSpan.Zero;
            var topics = reader.ReadCollection(ReadFetchResponseTopic);
            return new KafkaFetchResponse(topics, throttleTime);
        }

        private KafkaFetchResponseTopic ReadFetchResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadFetchResponseTopicPartition);

            return new KafkaFetchResponseTopic(topicName, partitions);
        }

        private KafkaFetchResponseTopicPartition ReadFetchResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            // ReSharper disable UnusedVariable

            var partitionId = reader.ReadInt32();
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var highwaterMarkOffset = reader.ReadInt64();

            var messages = new List<KafkaMessageAndOffset>();

            var requiredMessageSetSize = reader.BeginReadSize();
            var actualMessageSetSize = 0;
            
            while (reader.CanRead() && (actualMessageSetSize = reader.EndReadSize()) < requiredMessageSetSize)
            {
                var offset = reader.ReadInt64();

                var messageRequiredSize = reader.BeginReadSize();                
                if ((messageRequiredSize <= 0 ||
                    (requiredMessageSetSize - actualMessageSetSize) < messageRequiredSize))
                {
                    reader.SkipData(requiredMessageSetSize - actualMessageSetSize);
                    actualMessageSetSize = reader.EndReadSize();
                    break;
                }

                var messageRequiredCrc = reader.BeginReadCrc32();
                
                var messageMagicByte = reader.ReadInt8();
                var messageAttribute = reader.ReadInt8();
                var messageTimestampUtc = messageMagicByte == MessageMagicByteV010 ? reader.ReadNulalbleTimestampUtc() : null;
                var messageKey = reader.ReadByteArray();

                var messageCodecAttribute = messageAttribute & MessageAttributeCodecMask;
                var messageTimestampAttribute = messageAttribute & MessageAttributeTimestampMask; //todo (v10) use timestamp?

                if (messageCodecAttribute == MessageCodecGZipAttribute)
                {
                    // gzip message
                    var nestedRequiredMessageSetSize = reader.BeginReadGZipData();
                    int nestedActualMessageSetSize;
                    while ((nestedActualMessageSetSize = reader.EndReadGZipData()) < nestedRequiredMessageSetSize)
                    {
                        // nested message set
                        var nestedOffset = reader.ReadInt64();                        

                        var nestedRequiredSize = reader.BeginReadSize();
                        if (messageRequiredSize <= 0 ||
                            (nestedRequiredMessageSetSize - nestedActualMessageSetSize) < messageRequiredSize)
                        {
                            reader.SkipData(nestedRequiredMessageSetSize - nestedActualMessageSetSize);
                            nestedActualMessageSetSize = reader.EndReadGZipData();
                            break;
                        }
                        
                        var nestedRequiredCrc = reader.BeginReadCrc32();

                        var nestedMagicByte = reader.ReadInt8();
                        var nestedAttribute = reader.ReadInt8();
                        var nestedTimestampUtc = messageMagicByte == MessageMagicByteV010 ? reader.ReadNulalbleTimestampUtc() : null;
                        var nestedKey = reader.ReadByteArray();
                        var nestedValue = reader.ReadByteArray();

                        var nestedActualCrc = reader.EndReadCrc32();
                        var nestedActualSize = reader.EndReadSize();

                        if (nestedActualSize != nestedRequiredSize)
                        {                            
                            throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidMessageSize);
                        }

                        if (nestedActualCrc != nestedRequiredCrc)
                        {
                            //todo (E013) invalid CRC
                            continue;
                        }                        
                        
                        var nestedMessage = new KafkaMessageAndOffset(nestedOffset, nestedKey, nestedValue);
                        messages.Add(nestedMessage);
                    }

                    if (nestedActualMessageSetSize != nestedRequiredMessageSetSize)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
                    }
                }
                else
                {
                    //ordinary message
                    var value = reader.ReadByteArray();

                    var messageActualCrc = reader.EndReadCrc32();
                    var messageActualSize = reader.EndReadSize();

                    if (messageActualSize != messageRequiredSize)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
                    }

                    if (messageActualCrc != messageRequiredCrc)
                    {
                        //todo (E013) invalid CRC
                        continue;
                    }                    
                    var message = new KafkaMessageAndOffset(offset, messageKey, value);
                    messages.Add(message);
                }
            }

            if (actualMessageSetSize != requiredMessageSetSize)
            {
                //todo
            }
            
            return new KafkaFetchResponseTopicPartition(partitionId, errorCode, highwaterMarkOffset, messages);
            // ReSharper enable UnusedVariable
        }

        #endregion FetchResponse
    }
}
