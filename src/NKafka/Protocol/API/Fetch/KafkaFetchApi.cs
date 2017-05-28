using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    internal sealed class KafkaFetchApi : IKafkaRequestApi
    {
        const byte MessageMagicByteV09 = 0;
        const byte MessageMagicByteV010 = 1;

        const byte MessageEmptyAttribute = 0;

        const byte MessageAttributeCodecMask = 0x7;
        const byte MessageCodecNoneAttribute = MessageAttributeCodecMask & (byte)KafkaCodecType.CodecNone;
        const byte MessageCodecGZipAttribute = MessageAttributeCodecMask & (byte)KafkaCodecType.CodecGzip;

        const byte MessageAttributeTimestampMask = 0x8;
        const byte MessageTimestampLogAppendTimeAttribute = MessageAttributeTimestampMask & (byte)KafkaTimestampType.LogAppendTime << 3;
        const byte MessageTimestampCreateTimeAttribute = MessageAttributeTimestampMask & (byte)KafkaTimestampType.CreateTime << 3;

        const int MessageHeaderSize = 12;

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

        private void WriteFetchRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaFetchRequest request)
        {
            writer.WriteNullableInt32(request.ReplicaId);
            writer.WriteInt32((int)(request.MaxWaitTime.TotalMilliseconds));
            writer.WriteInt32(request.MinBytes);
            if (_requestVersion >= KafkaRequestVersion.V3)
            {
                writer.WriteInt32(request.MaxBytes ?? int.MaxValue);
            }
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

        [NotNull]
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

            var messageSetRequiredSize = reader.BeginReadSize();
            var messageSetActualSize = 0;

            while (reader.CanRead() && (messageSetActualSize < messageSetRequiredSize))
            {
                if (messageSetRequiredSize - messageSetActualSize < MessageHeaderSize)
                {
                    reader.SkipData(messageSetRequiredSize - messageSetActualSize);
                    messageSetActualSize = reader.EndReadSize();
                    break;
                }

                var messageOffset = reader.ReadInt64();
                var messageRequiredSize = reader.BeginReadSize();
                messageSetActualSize += MessageHeaderSize;

                if (messageRequiredSize <= 0 ||
                    messageRequiredSize > messageSetRequiredSize - messageSetActualSize)
                {
                    reader.SkipData(messageSetRequiredSize - messageSetActualSize);
                    reader.CancelReadSize();
                    messageSetActualSize = reader.EndReadSize();
                    break;
                }

                var messageRequiredCrc = reader.BeginReadCrc32();

                var messageMagicByte = reader.ReadInt8();
                var messageAttribute = reader.ReadInt8();
                var messageTimestampUtc = messageMagicByte == MessageMagicByteV010 ? reader.ReadNulalbleTimestampUtc() : null;
                var messageKey = reader.ReadByteArray();

                var messageCodecAttribute = messageAttribute & MessageAttributeCodecMask;
                var messageTimestampAttribute = messageAttribute & MessageAttributeTimestampMask;

                if (messageCodecAttribute == MessageCodecGZipAttribute)
                {
                    // gzip message
                    var nestedMessageSetRequiredSize = reader.BeginReadGZipData();
                    var nestedMessageSetActualSize = 0;
                    while (reader.CanRead() && (nestedMessageSetActualSize < nestedMessageSetRequiredSize))
                    {
                        if (nestedMessageSetRequiredSize - nestedMessageSetActualSize < MessageHeaderSize)
                        {
                            reader.SkipData(messageSetRequiredSize - messageSetActualSize);
                            nestedMessageSetActualSize = reader.EndReadSize();
                            break;
                        }

                        // nested message set
                        var nestedMessageOffset = reader.ReadInt64();
                        var nestedMessageRequiredSize = reader.BeginReadSize();
                        nestedMessageSetActualSize += MessageHeaderSize;

                        if (nestedMessageRequiredSize <= 0 ||
                            nestedMessageRequiredSize > nestedMessageSetRequiredSize - nestedMessageSetActualSize)
                        {
                            reader.SkipData(nestedMessageSetRequiredSize - nestedMessageSetActualSize);
                            reader.CancelReadSize();
                            nestedMessageSetActualSize = reader.EndReadGZipData();
                            break;
                        }

                        var nestedMessageRequiredCrc = reader.BeginReadCrc32();

                        var nestedMessageMagicByte = reader.ReadInt8();
                        if (nestedMessageMagicByte == MessageMagicByteV010)
                        {
                            nestedMessageOffset = messageOffset + nestedMessageOffset;
                        }

                        var nestedMessageAttribute = reader.ReadInt8();
                        var nestedMessageTimestampUtc = messageMagicByte == MessageMagicByteV010 ? reader.ReadNulalbleTimestampUtc() : null;
                        var nestedMessageKey = reader.ReadByteArray();
                        var nestedMessageValue = reader.ReadByteArray();

                        var nestedMessageActualCrc = reader.EndReadCrc32();
                        var nestedMessageActualSize = reader.EndReadSize();

                        if (nestedMessageActualSize != nestedMessageRequiredSize)
                        {
                            throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedMessageSize);
                        }

                        if (nestedMessageActualCrc != nestedMessageRequiredCrc)
                        {
                            throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidMessageCrc);
                        }

                        var nestedMessage = new KafkaMessageAndOffset(nestedMessageOffset, nestedMessageKey, nestedMessageValue, nestedMessageTimestampUtc);
                        messages.Add(nestedMessage);
                        nestedMessageSetActualSize = reader.EndReadGZipData();
                    }

                    if (nestedMessageSetActualSize != nestedMessageSetRequiredSize)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedDataSize);
                    }

                    var messageActualCrc = reader.EndReadCrc32();
                    var messageActualSize = reader.EndReadSize();

                    if (messageActualSize != messageRequiredSize)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedMessageSize);
                    }

                    if (messageActualCrc != messageRequiredCrc)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidMessageCrc);
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
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedMessageSize);
                    }

                    if (messageActualCrc != messageRequiredCrc)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidMessageCrc);
                    }
                    var message = new KafkaMessageAndOffset(messageOffset, messageKey, value, messageTimestampUtc);
                    messages.Add(message);
                }
                messageSetActualSize = reader.EndReadSize();
            }

            if (messageSetActualSize != messageSetRequiredSize)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.UnexpectedDataSize);
            }

            return new KafkaFetchResponseTopicPartition(partitionId, errorCode, highwaterMarkOffset, messages);
            // ReSharper enable UnusedVariable
        }

        #endregion FetchResponse
    }
}