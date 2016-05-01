using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public KafkaClientTopicMetadataInfo TopicMetadataInfo;

        [NotNull] private readonly KafkaProducerSettings _settings;

        [NotNull] private readonly IKafkaProducerTopicBuffer _buffer;
        [NotNull] private IReadOnlyList<int> _topicPartitionIds;
        [NotNull] private IReadOnlyDictionary<int, KafkaProducerTopicPartition> _topicPartitions;

        public KafkaProducerTopic([NotNull] string topicName,
            [NotNull] KafkaProducerSettings settings,
            [NotNull] IKafkaProducerTopicBuffer buffer)
        {
            TopicName = topicName;
            _settings = settings;
            _buffer = buffer;
            _topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>();
            _topicPartitionIds = new int[0];
            TopicMetadataInfo = new KafkaClientTopicMetadataInfo(topicName, DateTime.UtcNow, false, null, null);
        }

        [NotNull]
        public KafkaProducerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaProducerTopicPartition(TopicName, partitionId, _settings, _buffer.FallbackHandler);
        }
        
        public void ApplyPartitions([NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartition> partitions)
        {
            var topicPartitionIds = new List<int>(partitions.Count);
            var topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>(partitions.Count);            

            foreach (var partition in partitions)
            {
                topicPartitionIds.Add(partition.PartitonId);
                topicPartitions[partition.PartitonId] = partition;
            }

            _topicPartitionIds = topicPartitionIds;
            _topicPartitions = topicPartitions;
        }
        
        public void Flush()
        {
            _buffer.Flush(_topicPartitionIds, _topicPartitions);
        }

        [NotNull]
        public KafkaProducerTopicInfo GetDiagnosticsInfo()
        {
            var partitionInfos = new List<KafkaProducerTopicPartitionInfo>(_topicPartitions.Count);
            
            long topicTotalEnqueuedMessageCount = 0;

            long topicTotalFallbackMessageCount = 0;            
            var topicSendMessageTimestampUtc = (DateTime?) null;

            int topicSendPendingMessageCount = 0;
            long topicTotalSentMessageCount = 0;
            var topicFallbackMessageTimestampUtc = (DateTime?)null;

            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;
                
                var partitionTotalEnqueuedCount = partition.TotalEnqueuedCount;
                var partitionEnqueueTimestampUtc = partition.EnqueueTimestampUtc;

                var partitionFallbackMessageCount = partition.TotalFallbackCount;
                var partitionFallbackMessageTimestampUtc = partition.FallbackTimestampUtc;

                var partitionSendPendingCount = partition.SendPendingCount + partitionBroker.RetryEnqueuedMessageCount;
                var partitionSentMessageCount = partitionBroker.TotalSentMessageCount;
                var partitionSendMessageTimestampUtc = partitionBroker.SendMessageTimestampUtc;
                
                topicTotalEnqueuedMessageCount += partitionTotalEnqueuedCount;

                topicTotalFallbackMessageCount += partitionFallbackMessageCount;
                if (topicFallbackMessageTimestampUtc == null || topicFallbackMessageTimestampUtc < partitionFallbackMessageTimestampUtc)
                {
                    topicFallbackMessageTimestampUtc = partitionFallbackMessageTimestampUtc;
                }

                topicSendPendingMessageCount += partitionSendPendingCount;
                topicTotalSentMessageCount += partitionSentMessageCount;                
                if (topicSendMessageTimestampUtc == null || topicSendMessageTimestampUtc < partitionSendMessageTimestampUtc)
                {
                    topicSendMessageTimestampUtc = partitionSendMessageTimestampUtc;
                }

                var partitionMessagesInfo = new KafkaProducerTopicMessagesInfo(
                    partitionTotalEnqueuedCount, partitionEnqueueTimestampUtc,
                    partitionSendPendingCount, partitionSentMessageCount, partitionSendMessageTimestampUtc,
                    partitionFallbackMessageCount, partitionFallbackMessageTimestampUtc);

                var partitionInfo = new KafkaProducerTopicPartitionInfo(partition.PartitonId,
                    partitionBroker.IsReady,
                    partitionBroker.Error, partitionBroker.ErrorTimestampUtc,
                    partitionMessagesInfo,
                    partitionBroker.LimitInfo); 
                partitionInfos.Add(partitionInfo);
            }

            var metadataInfo = TopicMetadataInfo;

            var bufferEnqueuedCount = _buffer.EnqueuedCount;
            topicSendPendingMessageCount += _buffer.EnqueuedCount;
            topicTotalEnqueuedMessageCount += bufferEnqueuedCount;

            var topicMessagesInfo = new KafkaProducerTopicMessagesInfo(
                topicTotalEnqueuedMessageCount, _buffer.EnqueueTimestampUtc,
                topicSendPendingMessageCount, topicTotalSentMessageCount, topicSendMessageTimestampUtc,
                topicTotalFallbackMessageCount, topicFallbackMessageTimestampUtc);

            return new KafkaProducerTopicInfo(
                TopicName,
                metadataInfo,
                DateTime.UtcNow,
                metadataInfo.IsReady,
                topicMessagesInfo,
                partitionInfos);
        }
    }
}
