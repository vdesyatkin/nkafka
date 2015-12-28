using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.Offset
{
    internal static class KafkaOffsetApi
    {
        #region OffsetRequest

        [PublicAPI]
        public static void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request)
        {
            WriteOffsetRequest(writer, (KafkaOffsetRequest)request);
        }

        private static void WriteOffsetRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetRequest request)
        {
            writer.WriteNullableInt32(request.ReplicaId);            
            writer.WriteCollection(request.Topics, WriteOffsetRequestTopic);
        }

        private static void WriteOffsetRequestTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetRequestTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteOffsetRequestTopicPartition);
        }

        private static void WriteOffsetRequestTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetRequestTopicPartition partition)
        {
            writer.WriteInt32(partition.PartitionId);
            writer.WriteNullableInt64(partition.Period != null ? (long)Math.Round(partition.Period.Value.TotalMilliseconds) : (long?)null);
            writer.WriteInt32(partition.MaxNumberOfOffsets);
        }

        #endregion OffsetRequest

        #region OffsetResponse

        [PublicAPI]
        public static IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader)
        {
            return ReadOffsetResponse(reader);
        }

        private static KafkaOffsetResponse ReadOffsetResponse([NotNull] KafkaBinaryReader reader)
        {
            var topics = reader.ReadCollection(ReadOffsetResponseTopic);
            return new KafkaOffsetResponse(topics);
        }

        private static KafkaOffsetResponseTopic ReadOffsetResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadOffsetResponseTopicPartition);

            return new KafkaOffsetResponseTopic(topicName, partitions);
        }

        private static KafkaOffsetResponseTopicPartition ReadOffsetResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var offsets = reader.ReadCollection(reader.ReadInt64);
            return new KafkaOffsetResponseTopicPartition(partitionId, errorCode, offsets);
        }

        #endregion OffsetResponse
    }
}
