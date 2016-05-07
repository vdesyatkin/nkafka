using System;
using JetBrains.Annotations;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol.API.TopicMetadata
{
    [PublicAPI]
    internal class KafkaTopicMetadataApi : IKafkaRequestApi
    {
        public Type RequestType => typeof(KafkaTopicMetadataRequest);

        private readonly KafkaRequestVersion _requestVersion;

        public KafkaTopicMetadataApi(KafkaRequestVersion requestVersion)
        {
            _requestVersion = requestVersion;
        }

        #region TopicMetadataRequest

        public void WriteRequest(KafkaBinaryWriter writer, IKafkaRequest request)
        {
            WriteTopicMetadataRequest(writer, (KafkaTopicMetadataRequest)request);
        }

        private static void WriteTopicMetadataRequest([NotNull] KafkaBinaryWriter writer, [NotNull] KafkaTopicMetadataRequest request)
        {            
            writer.WriteCollection(request.TopicNames, writer.WriteString);
        }
       
        public static KafkaTopicMetadataRequest ReadRequest([NotNull] KafkaBinaryReader reader)
        {
            var topicNames = reader.ReadCollection(reader.ReadString);
            return new KafkaTopicMetadataRequest(topicNames);
        }

        #endregion TopicMetadataRequest

        #region TopicMetadataResponse
     
        public IKafkaResponse ReadResponse(KafkaBinaryReader reader)
        {
            return ReadTopicMetadataResponse(reader);
        }

        private KafkaTopicMetadataResponse ReadTopicMetadataResponse([NotNull] KafkaBinaryReader reader)
        {
            var brokers = reader.ReadCollection(ReadResponseBroker);
            var controllerBrokerId = _requestVersion >= KafkaRequestVersion.V1 ? reader.ReadInt32() : (int?)null;
            var topics = reader.ReadCollection(ReadResponseTopic);
            return new KafkaTopicMetadataResponse(controllerBrokerId, brokers, topics);
        }

        private KafkaTopicMetadataResponseBroker ReadResponseBroker([NotNull] KafkaBinaryReader reader)
        {
            var brokerId = reader.ReadInt32();
            var host = reader.ReadString();
            var port = reader.ReadInt32();
            var rack = _requestVersion >= KafkaRequestVersion.V1 ? reader.ReadString() : null;

            return new KafkaTopicMetadataResponseBroker(brokerId, host, port, rack);
        }

        private KafkaTopicMetadataResponseTopic ReadResponseTopic([NotNull] KafkaBinaryReader reader)
        {
            var errorCode = (KafkaResponseErrorCode)reader.ReadInt16();
            var topicName = reader.ReadString();
            var isInternalTopic = _requestVersion >= KafkaRequestVersion.V1 ? reader.ReadBool() : (bool?)null;
            var partitions = reader.ReadCollection(ReadResponseTopicPartition);           

            return new KafkaTopicMetadataResponseTopic(errorCode, topicName, isInternalTopic, partitions);
        }

        private KafkaTopicMetadataResponseTopicPartition ReadResponseTopicPartition([NotNull] KafkaBinaryReader reader)
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
