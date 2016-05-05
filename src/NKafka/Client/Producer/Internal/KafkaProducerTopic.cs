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
            TopicMetadataInfo = new KafkaClientTopicMetadataInfo(topicName, false, null, null, DateTime.UtcNow);
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

            long topicSendPendingMessageCount = 0;
            long topicTotalSentMessageCount = 0;
            var topicFallbackMessageTimestampUtc = (DateTime?)null;

            var topicTotalEnqueuedMessageSizeBytes = 0;
            var topicTotalSentMessageSizeBytes = 0;
            var topicSendPendingMessageSizeBytes = 0;

            var topicIsReady = true;

            //todo (E008) size statistics            
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                
                var partitionBroker = partition.BrokerPartition;
                topicIsReady = topicIsReady && partitionBroker.IsReady;

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

                var partitionMessageCountInfo = new KafkaProducerTopicMessageCountInfo(
                    partitionTotalEnqueuedCount, partitionEnqueueTimestampUtc,
                    partitionSendPendingCount, partitionSentMessageCount, partitionSendMessageTimestampUtc,
                    partitionFallbackMessageCount, partitionFallbackMessageTimestampUtc);

                var partitionTotalEnqueuedMessageSizeBytes = 0; //todo (E008)
                var partitionTotalSentMessageSizeBytes = 0;
                var partitionSendPendingMessageSizeBytes = 0;

                topicTotalEnqueuedMessageSizeBytes += partitionTotalEnqueuedMessageSizeBytes;
                topicTotalSentMessageSizeBytes += partitionTotalSentMessageSizeBytes;
                topicSendPendingMessageSizeBytes += partitionSendPendingMessageSizeBytes;

                var partitionMessageSizeInfo = new KafkaProducerTopicMessageSizeInfo(
                    partitionTotalEnqueuedMessageSizeBytes, partitionTotalSentMessageSizeBytes, partitionSendPendingMessageSizeBytes);

                var partitionInfo = new KafkaProducerTopicPartitionInfo(partition.PartitonId,
                    partitionBroker.IsReady,
                    partitionBroker.Error, partitionBroker.ErrorTimestampUtc,
                    partitionMessageCountInfo,
                    partitionMessageSizeInfo,
                    partitionBroker.LimitInfo); 
                partitionInfos.Add(partitionInfo);
            }

            var metadataInfo = TopicMetadataInfo;

            var bufferEnqueuedCount = _buffer.EnqueuedCount;
            topicSendPendingMessageCount += _buffer.EnqueuedCount;
            topicTotalEnqueuedMessageCount += bufferEnqueuedCount;

            var topicMessageCountInfo = new KafkaProducerTopicMessageCountInfo(
                topicTotalEnqueuedMessageCount, _buffer.EnqueueTimestampUtc,
                topicSendPendingMessageCount, topicTotalSentMessageCount, topicSendMessageTimestampUtc,
                topicTotalFallbackMessageCount, topicFallbackMessageTimestampUtc);

            var topicMessageSizeInfo = new KafkaProducerTopicMessageSizeInfo(
                topicTotalEnqueuedMessageSizeBytes, topicTotalSentMessageSizeBytes, topicSendPendingMessageSizeBytes);

            topicIsReady = topicIsReady && metadataInfo.IsReady;

            return new KafkaProducerTopicInfo(
                TopicName,
                topicIsReady,
                metadataInfo,                                
                topicMessageCountInfo,
                topicMessageSizeInfo,
                partitionInfos,
                DateTime.UtcNow);
        }
    }
}
