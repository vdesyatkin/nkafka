using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    internal sealed class KafkaOffsetCommitApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaOffsetCommitRequest);

        #region OffsetCommitRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteOffsetCommitRequest(writer, (KafkaOffsetCommitRequest)request);
        }

        private static void WriteOffsetCommitRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetCommitRequest request)
        {
            writer.WriteString(request.GroupName);
            writer.WriteInt32(request.GroupGenerationId);
            writer.WriteString(request.MemberId);
            writer.WriteInt64((int)request.RetentionTime.TotalMilliseconds);

            writer.WriteCollection(request.Topics, WriteOffsetCommitRequestTopic);
        }

        private static void WriteOffsetCommitRequestTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetCommitRequestTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteOffsetCommitRequestTopicPartition);
        }

        private static void WriteOffsetCommitRequestTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetCommitRequestTopicPartition partition)
        {
            writer.WriteInt32(partition.PartitionId);
            writer.WriteInt64(partition.Offset);
            writer.WriteString(partition.Metadata);            
        }

        #endregion OffsetCommitRequest

        #region OffsetCommitResponse

        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadOffsetCommitResponse(reader);
        }

        private static KafkaOffsetCommitResponse ReadOffsetCommitResponse([NotNull] KafkaBinaryReader reader)
        {
            var topics = reader.ReadCollection(ReadOffsetCommitResponseTopic);
            return new KafkaOffsetCommitResponse(topics);
        }

        private static KafkaOffsetCommitResponseTopic ReadOffsetCommitResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadOffsetCommitResponseTopicPartition);

            return new KafkaOffsetCommitResponseTopic(topicName, partitions);
        }

        private static KafkaOffsetCommitResponseTopicPartition ReadOffsetCommitResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();            
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();

            return new KafkaOffsetCommitResponseTopicPartition(partitionId, errorCode);
        }

        #endregion OffsetCommitResponse
    }
}
