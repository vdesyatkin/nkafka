using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    internal sealed class KafkaOffsetFetchApi : IKafkaRequestApi
    {        
        private readonly KafkaRequestVersion _requestVersion;

        public KafkaOffsetFetchApi(KafkaRequestVersion requestVersion)
        {            
            _requestVersion = requestVersion;
        }

        #region OffsetFetchRequest
        
        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteOffsetFetchRequest(writer, (KafkaOffsetFetchRequest)request);
        }

        private static void WriteOffsetFetchRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetFetchRequest request)
        {
            writer.WriteString(request.GroupName);          
            writer.WriteCollection(request.Topics, WriteOffsetFetchRequestTopic);
        }

        private static void WriteOffsetFetchRequestTopic([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaOffsetFetchRequestTopic topic)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteCollection(topic.PartitionIds, writer.WriteInt32);            
        }

        #endregion OffsetFetchRequest

        #region OffsetFetchResponse

        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadOffsetFetchResponse(reader);
        }

        [NotNull]
        private KafkaOffsetFetchResponse ReadOffsetFetchResponse([NotNull] KafkaBinaryReader reader)
        {            
            var topics = reader.ReadCollection(ReadOffsetFetchResponseTopic);
            var errorCode = _requestVersion >= KafkaRequestVersion.V2 ? (KafkaResponseErrorCode) reader.ReadInt16() : KafkaResponseErrorCode.NoError;
            return new KafkaOffsetFetchResponse(topics, errorCode);
        }

        private static KafkaOffsetFetchResponseTopic ReadOffsetFetchResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var topicName = reader.ReadString();
            var partitions = reader.ReadCollection(ReadOffsetFetchResponseTopicPartition);

            return new KafkaOffsetFetchResponseTopic(topicName, partitions);
        }

        private static KafkaOffsetFetchResponseTopicPartition ReadOffsetFetchResponseTopicPartition([NotNull] KafkaBinaryReader reader)
        {           
            var partitionId = reader.ReadInt32();
            var offset = reader.ReadInt64();
            var metadata = reader.ReadString();
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();

            return new KafkaOffsetFetchResponseTopicPartition(partitionId, errorCode, offset >= 0 ? offset : (long?)null, metadata);
        }

        #endregion OffsetFetchResponse
    }
}