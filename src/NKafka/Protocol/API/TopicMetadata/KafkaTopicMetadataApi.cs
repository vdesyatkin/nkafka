using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.TopicMetadata
{
    internal static class KafkaTopicMetadataApi
    {
        #region TopicMetadataRequest

        [PublicAPI]
        public static void WriteRequest([NotNull] KafkaBinaryWriter writer, [NotNull] IKafkaRequest request)
        {
            WriteTopicMetadataRequest(writer, (KafkaTopicMetadataRequest)request);
        }

        private static void WriteTopicMetadataRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataRequest request)
        {            
            writer.WriteCollection(request.TopicNames, writer.WriteString);
        }

        [PublicAPI]
        public static KafkaTopicMetadataRequest ReadRequest([NotNull] KafkaBinaryReader reader)
        {
            var topicNames = reader.ReadCollection(reader.ReadString);
            return new KafkaTopicMetadataRequest(topicNames);
        }

        #endregion TopicMetadataRequest

        #region TopicMetadataResponse

        public static void WriteResponse([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataResponse response)
        {
            writer.WriteCollection(response.Brokers, WriteResponseBroker);
            writer.WriteCollection(response.Topics, WriteResponseTopic);
        }

        private static void WriteResponseBroker([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataResponseBroker broker)
        {
            writer.WriteInt32(broker.BrokerId);
            writer.WriteString(broker.Host);
            writer.WriteInt32(broker.Port);
        }

        private static void WriteResponseTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataResponseTopic topic)
        {
            writer.WriteInt16((short)topic.ErrorCode);
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.Partitions, WriteResponseTopicPartition);
        }

        private static void WriteResponseTopicPartition([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataResponseTopicPartition partition)
        {
            writer.WriteInt16((short)partition.ErrorCode);
            writer.WriteInt32(partition.PartitionId);
            writer.WriteInt32(partition.LeaderId);
            writer.WriteCollection(partition.ReplicaIds, writer.WriteInt32);
            writer.WriteCollection(partition.Isr, writer.WriteInt32);
        }
        
        [PublicAPI]
        public static IKafkaResponse ReadResponse([NotNull] KafkaBinaryReader reader)
        {
            return ReadTopicMetadataResponse(reader);
        }

        private static KafkaTopicMetadataResponse ReadTopicMetadataResponse([NotNull] KafkaBinaryReader reader)
        {
            var brokers = reader.ReadCollection(ReadResponseBroker);
            var topics = reader.ReadCollection(ReadResponseTopic);
            return new KafkaTopicMetadataResponse(brokers, topics);
        }

        private static KafkaTopicMetadataResponseBroker ReadResponseBroker([NotNull] KafkaBinaryReader reader)
        {
            var brokerId = reader.ReadInt32();
            var host = reader.ReadString();
            var port = reader.ReadInt32();

            return new KafkaTopicMetadataResponseBroker(brokerId, host, port);
        }

        private static KafkaTopicMetadataResponseTopic ReadResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadResponseTopicPartition);            

            return new KafkaTopicMetadataResponseTopic(errorCode, topicName, partitions);
        }

        private static KafkaTopicMetadataResponseTopicPartition ReadResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode) reader.ReadInt16();
            var partitionId = reader.ReadInt32();
            var leaderId = reader.ReadInt32();
            var replicaIds = reader.ReadCollection(reader.ReadInt32);
            var isr = reader.ReadCollection(reader.ReadInt32);

            return new KafkaTopicMetadataResponseTopicPartition(errorCode, partitionId, leaderId, replicaIds, isr);
        }

        #endregion MetadataResponse
    }
}
