using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    internal sealed class KafkaOffsetFetchApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaOffsetFetchRequest);

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

        private static KafkaOffsetFetchResponse ReadOffsetFetchResponse([NotNull] KafkaBinaryReader reader)
        {            
            var topics = reader.ReadCollection(ReadOffsetFetchResponseTopic);
            return new KafkaOffsetFetchResponse(topics);
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

            return new KafkaOffsetFetchResponseTopicPartition(partitionId, errorCode, offset, metadata);
        }

        #endregion OffsetFetchResponse
    }
}
