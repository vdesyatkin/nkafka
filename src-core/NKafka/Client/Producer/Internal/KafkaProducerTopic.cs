using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Client.Producer.Diagnostics;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public KafkaClientTopicMetadataInfo TopicMetadataInfo;

        [NotNull] private readonly KafkaProducerSettings _settings;
        [CanBeNull] private readonly IKafkaProducerTopicLogger _logger;

        [NotNull] private readonly IKafkaProducerTopicBuffer _buffer;
        [NotNull] private IReadOnlyList<int> _topicPartitionIds;
        [NotNull] private IReadOnlyDictionary<int, KafkaProducerTopicPartition> _topicPartitions;

        public KafkaProducerTopic([NotNull] string topicName,
            [NotNull] KafkaProducerSettings settings,
            [NotNull] IKafkaProducerTopicBuffer buffer,
            [CanBeNull] IKafkaProducerTopicLogger logger)
        {
            TopicName = topicName;
            _settings = settings;            
            _buffer = buffer;
            _logger = logger;
            _topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>();
            _topicPartitionIds = new int[0];
            TopicMetadataInfo = new KafkaClientTopicMetadataInfo(topicName, false, null, null, DateTime.UtcNow);
        }

        [NotNull]
        public KafkaProducerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaProducerTopicPartition(TopicName, partitionId, _settings, _buffer.FallbackHandler, _logger);
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
        
        public void DistributeMessagesByPartitions()
        {
            _buffer.DistributeMessagesByPartitions(_topicPartitionIds, _topicPartitions);
        }

        #region Diagnostics

        public bool IsReady
        {
            get
            {
                bool? isReady = null;
                foreach (var partition in _topicPartitions)
                {
                    var partitionBroker = partition.Value?.BrokerPartition;
                    if (partitionBroker == null) continue;
                    isReady = (isReady ?? true) && partitionBroker.IsReady;
                }

                return isReady == true;
            }
        }

        public bool IsSynchronized
        {
            get
            {
                bool isSynchronized = true;
                foreach (var partition in _topicPartitions)
                {
                    var partitionBroker = partition.Value?.BrokerPartition;
                    if (partitionBroker == null) continue;
                    isSynchronized = isSynchronized && partitionBroker.IsSynchronized;
                }

                return isSynchronized && _buffer.EnqueuedCount == 0;
            }
        }

        [NotNull]
        public KafkaProducerTopicInfo GetDiagnosticsInfo()
        {
            var partitionInfos = new List<KafkaProducerTopicPartitionInfo>(_topicPartitions.Count);
            
            long topicTotalEnqueuedMessageCount = 0;

            long topicTotalFallbackMessageCount = 0;            
            var topicSendMessageTimestampUtc = (DateTime?) null;

            long topicSendPendingMessageCount = 0;
            long topicRetrySendPendingMessageCount = 0;
            long topicTotalSentMessageCount = 0;
            var topicFallbackMessageTimestampUtc = (DateTime?)null;

            long topicTotalEnqueuedMessageSizeBytes = 0;
            long topicTotalSentMessageSizeBytes = 0;
            long topicSendPendingMessageSizeBytes = 0;

            bool? topicIsReady = null;
            bool topicIsSynchronized = true;
            
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                
                var partitionBroker = partition.BrokerPartition;                

                var partitionTotalEnqueuedMessageCount = partitionBroker.TotalEnqueuedMessageCount;
                var partitionEnqueueTimestampUtc = partitionBroker.EnqueueTimestampUtc;

                var partitionFallbackMessageCount = partitionBroker.TotalFallbackMessageCount;
                var partitionFallbackMessageTimestampUtc = partitionBroker.FallbackTimestampUtc;

                var partitionSendPendingMessageCount = partitionBroker.SendPendingMessageMessageCount;
                var partitionRetrySendPendingMessageCount = partitionBroker.RetrySendPendingMessageCount;
                var partitionTotalSentMessageCount = partitionBroker.TotalSentMessageCount;
                var partitionSendMessageTimestampUtc = partitionBroker.SendMessageTimestampUtc;
                
                topicTotalEnqueuedMessageCount += partitionTotalEnqueuedMessageCount;

                topicTotalFallbackMessageCount += partitionFallbackMessageCount;
                if (topicFallbackMessageTimestampUtc == null || topicFallbackMessageTimestampUtc < partitionFallbackMessageTimestampUtc)
                {
                    topicFallbackMessageTimestampUtc = partitionFallbackMessageTimestampUtc;
                }

                topicSendPendingMessageCount += partitionSendPendingMessageCount;
                topicRetrySendPendingMessageCount += partitionRetrySendPendingMessageCount;
                topicTotalSentMessageCount += partitionTotalSentMessageCount;                
                if (topicSendMessageTimestampUtc == null || topicSendMessageTimestampUtc < partitionSendMessageTimestampUtc)
                {
                    topicSendMessageTimestampUtc = partitionSendMessageTimestampUtc;
                }                

                var partitionMessageCountInfo = new KafkaProducerTopicMessageCountInfo(
                    partitionTotalEnqueuedMessageCount, partitionEnqueueTimestampUtc,
                    partitionSendPendingMessageCount, partitionRetrySendPendingMessageCount, 
                    partitionTotalSentMessageCount, partitionSendMessageTimestampUtc,
                    partitionFallbackMessageCount, partitionFallbackMessageTimestampUtc);

                var partitionTotalEnqueuedMessageSizeBytes = partitionBroker.TotalEnqueuedMessageSizeBytes;
                var partitionTotalSentMessageSizeBytes = partitionBroker.TotalSentMessageSizeBytes;
                var partitionSendPendingMessageSizeBytes = partitionBroker.SendPendingMessageMessageSizeBytes;

                topicTotalEnqueuedMessageSizeBytes += partitionTotalEnqueuedMessageSizeBytes;
                topicTotalSentMessageSizeBytes += partitionTotalSentMessageSizeBytes;
                topicSendPendingMessageSizeBytes += partitionSendPendingMessageSizeBytes;

                var partitionMessageSizeInfo = new KafkaProducerTopicMessageSizeInfo(
                    partitionTotalEnqueuedMessageSizeBytes, partitionTotalSentMessageSizeBytes, partitionSendPendingMessageSizeBytes);

                var partitionIsReady = partitionBroker.IsReady;
                var partitionIsSynchronized = partitionBroker.IsSynchronized;

                topicIsReady = (topicIsReady ?? true) && partitionIsReady;
                topicIsSynchronized = topicIsSynchronized && partitionIsSynchronized;

                var partitionInfo = new KafkaProducerTopicPartitionInfo(partition.PartitonId,
                    partitionIsReady, partitionIsSynchronized,
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
                topicSendPendingMessageCount, topicRetrySendPendingMessageCount,
                topicTotalSentMessageCount, topicSendMessageTimestampUtc,
                topicTotalFallbackMessageCount, topicFallbackMessageTimestampUtc);

            var topicMessageSizeInfo = new KafkaProducerTopicMessageSizeInfo(
                topicTotalEnqueuedMessageSizeBytes, topicTotalSentMessageSizeBytes, topicSendPendingMessageSizeBytes);

            topicIsReady = topicIsReady == true && metadataInfo.IsReady;

            return new KafkaProducerTopicInfo(
                TopicName,
                topicIsReady == true, topicIsSynchronized,
                metadataInfo,                                
                topicMessageCountInfo,
                topicMessageSizeInfo,
                partitionInfos,
                DateTime.UtcNow);
        }

        #endregion Diagnostics
    }
}
