using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;
using NKafka.Client.Consumer.Logging;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic : IKafkaConsumerTopic
    {
        string IKafkaConsumerTopic.TopicName => TopicName;
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly string GroupName;
        [CanBeNull] public readonly string CatchUpGroupName;
        [NotNull] public KafkaClientTopicMetadataInfo TopicMetadataInfo;

        [CanBeNull] private KafkaConsumerGroupData _group;
        [CanBeNull] private readonly IKafkaConsumerFallbackHandler _fallbackHandler;
        [CanBeNull] private readonly IKafkaConsumerTopicLogger _logger;
        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull] private IReadOnlyDictionary<int, KafkaConsumerTopicPartition> _topicPartitions;
        [NotNull] private readonly ConcurrentDictionary<long, KafkaConsumerTopicPackageInfo> _packages;
        private long _currentPackageId;

        public KafkaConsumerTopic([NotNull] string topicName, 
            [NotNull] string groupName, [CanBeNull] string catchUpGroupName,
            [NotNull] KafkaConsumerSettings settings,
            [CanBeNull] IKafkaConsumerFallbackHandler fallbackHandler,
            [CanBeNull] IKafkaConsumerTopicLogger logger)
        { 
            TopicName = topicName;
            GroupName = groupName;
            Settings = settings;
            _fallbackHandler = fallbackHandler;
            _logger = logger;
            _topicPartitions = new Dictionary<int, KafkaConsumerTopicPartition>();
            _packages = new ConcurrentDictionary<long, KafkaConsumerTopicPackageInfo>();
            TopicMetadataInfo = new KafkaClientTopicMetadataInfo(topicName, false, null, null, DateTime.UtcNow);
        }

        [CanBeNull]
        public KafkaConsumerTopicPartition CreatePartition(int partitionId)
        {
            var group = _group;
            return group == null ? null : new KafkaConsumerTopicPartition(TopicName, partitionId, group, Settings, _fallbackHandler, _logger);
        }

        public void ApplyPartitions([NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerTopicPartition> partitions)
        {            
            var topicPartitions = new Dictionary<int, KafkaConsumerTopicPartition>(partitions.Count);

            foreach (var partition in partitions)
            {         
                topicPartitions[partition.PartitonId] = partition;
            }
           
            _topicPartitions = topicPartitions;
        }

        public void ApplyCoordinator([NotNull] IKafkaConsumerCoordinator groupCoordinator, [CanBeNull] IKafkaConsumerCoordinator catchUpGroupCoordinator)
        {
            _group = new KafkaConsumerGroupData(groupCoordinator, catchUpGroupCoordinator);
        }
        
        public IReadOnlyList<KafkaMessagePackage> Consume(int? maxMessageCount = null)
        {            
            var resultPackages = new List<KafkaMessagePackage>(_topicPartitions.Count);
            var totalMessageCount = 0;
            
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                var partitionBroker = partition.BrokerPartition;

                var partitionConsumePendingCount = partitionBroker.ConsumePendingMessageCount;                
                var partitionMessages = new List<KafkaMessage>(partitionConsumePendingCount);

                long beginOffset = 0;
                long endOffset = 0;

                KafkaMessageAndOffset messageAndOffset;
                while (partitionMessages.Count < partitionConsumePendingCount && partitionBroker.TryConsumeMessage(out messageAndOffset))
                {
                    if (messageAndOffset == null) continue;
                    
                    var message = new KafkaMessage(messageAndOffset.Key, messageAndOffset.Data, messageAndOffset.TiemestampUtc);

                    if (partitionMessages.Count == 0)
                    {
                        beginOffset = messageAndOffset.Offset;
                    }
                    partitionMessages.Add(message);
                    endOffset = messageAndOffset.Offset;                 

                    totalMessageCount++;
                    if (totalMessageCount >= maxMessageCount)
                    {
                        break;
                    }
                }
                if (partitionMessages.Count > 0)
                {
                    var packageId = Interlocked.Increment(ref _currentPackageId);                    
                    _packages[packageId] = new KafkaConsumerTopicPackageInfo(partition.PartitonId, beginOffset, endOffset);
                    resultPackages.Add(new KafkaMessagePackage(packageId, partition.PartitonId, beginOffset, endOffset, partitionMessages));
                }

                if (totalMessageCount >= maxMessageCount)
                {
                    break;
                }
            }

            return resultPackages;
        }

        public void EnqueueCommit(long packageId)
        {
            KafkaConsumerTopicPackageInfo package;
            if (!_packages.TryGetValue(packageId, out package) || package == null)
            {
                return;
            }

            KafkaConsumerTopicPartition partition;
            if (_topicPartitions.TryGetValue(package.PartitionId, out partition))
            {                
                partition?.SetCommitClientOffset(package.BeginOffset, package.EndOffset);
            }

            _packages.TryRemove(packageId, out package);
        }

        public long? GetCommitClientOffset(int partitionId)
        {
            KafkaConsumerTopicPartition partition;
            if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null) return null;
            return partition.GetCommitClientOffset();
        }

        #region Diagnostics

        public bool IsReady
        {
            get
            {
                bool? isReady = null;
                foreach (var partitionPair in _topicPartitions)
                {                    
                    var partitionBroker = partitionPair.Value?.BrokerPartition;
                    if (partitionBroker == null || !partitionBroker.IsAssigned) continue;
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
                foreach (var partitionPair in _topicPartitions)
                {
                    var partitionBroker = partitionPair.Value?.BrokerPartition;
                    if (partitionBroker == null || !partitionBroker.IsSynchronized) continue;
                    isSynchronized = isSynchronized && partitionBroker.IsSynchronized;
                }

                return isSynchronized;
            }
        }

        public KafkaConsumerTopicInfo GetDiagnosticsInfo()
        {
            var partitionInfos = new List<KafkaConsumerTopicPartitionInfo>(_topicPartitions.Count);

            long topicReceivePendingCount = 0;
            long topicTotalReceivedMessageCount = 0;
            var topicReceiveMessageTimestampUtc = (DateTime?)null;

            long topicConsumePendingCount = 0;
            long topicTotalConsumedMessageCount = 0;
            var topicConsumeMessageTimestampUtc = (DateTime?)null;

            long topicClientCommitPendingCount = 0;
            long topicTotalClientCommitedMessageCount = 0;
            var topicClientCommitMessageTimestampUtc = (DateTime?)null;

            long topicServerCommitPendingCount = 0;
            long topicTotalServerCommitedMessageCount = 0;
            var topicServerCommitMessageTimestampUtc = (DateTime?)null;

            long topicTotalReceivedMessageSizeBytes = 0;
            long topicTotalConsumedMessageSizeBytes = 0;
            long topicConsumePendingMessageSizeBytes = 0;
            long topicBufferedMessageSizeBytes = 0;

            bool? topicIsReady = null;
            var topicIsSynchronized = true;

            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;
                
                var partitionIsAssigned = partitionBroker.IsAssigned;
                var partitionIsReady = partitionBroker.IsReady;
                var partitionIsSynchronized = partitionBroker.IsSynchronized;         

                var partitionOffsetsInfo = partitionBroker.GetOffsetsInfo();

                var partitionReceivePendingCount = partitionOffsetsInfo.MaxAvailableServerOffset - (partitionOffsetsInfo.ReceivedClientOffset ?? partitionOffsetsInfo.CommitedServerOffset);
                if (partitionReceivePendingCount < 0)
                {
                    partitionReceivePendingCount = null;
                }
                var partitionTotalReceivedCount = partitionBroker.TotalReceivedMessageCount;
                var partitionReceivedTimestampUtc = partitionBroker.ReceiveTimestampUtc;

                var partitionConsumePendingCount = partitionBroker.ConsumePendingMessageCount;
                var partitionConsumeMessageCount = partitionBroker.TotalConsumedMessageCount;
                var partitionConsumeMessageTimestampUtc = partitionBroker.ConsumeTimestampUtc;

                var partitionClientCommitMessageCount = partition.TotalClientCommitedCount;
                var partitionClientCommitPendingCount = partitionConsumeMessageCount - partitionClientCommitMessageCount;
                if (partitionClientCommitPendingCount < 0)
                {
                    partitionClientCommitPendingCount = 0;
                }
                var partitionClientCommitMessageTimestampUtc = partition.ClientCommitTimestampUtc;

                var partitionServerCommitPendingCount = (partitionOffsetsInfo.CommitedServerOffset - partitionOffsetsInfo.CommitedServerOffset) ?? partitionClientCommitMessageCount;
                if (partitionServerCommitPendingCount < 0)
                {
                    partitionServerCommitPendingCount = 0;
                }
                var partitionServerCommitMessageCount = partitionClientCommitMessageCount - partitionServerCommitPendingCount;
                var partitionServerCommitMessageTimestampUtc = partition.CommitServerOffsetTimestampUtc;

                topicTotalReceivedMessageCount += partitionTotalReceivedCount;
                if (topicReceiveMessageTimestampUtc == null || topicReceiveMessageTimestampUtc < partitionReceivedTimestampUtc)
                {
                    topicReceiveMessageTimestampUtc = partitionReceivedTimestampUtc;
                }

                topicTotalConsumedMessageCount += partitionConsumeMessageCount;
                if (topicConsumeMessageTimestampUtc == null || topicConsumeMessageTimestampUtc < partitionConsumeMessageTimestampUtc)
                {
                    topicConsumeMessageTimestampUtc = partitionConsumeMessageTimestampUtc;
                }

                topicTotalClientCommitedMessageCount += partitionClientCommitMessageCount;
                if (topicClientCommitMessageTimestampUtc == null || topicClientCommitMessageTimestampUtc < partitionClientCommitMessageTimestampUtc)
                {
                    topicClientCommitMessageTimestampUtc = partitionClientCommitMessageTimestampUtc;
                }
                
                topicTotalServerCommitedMessageCount += partitionServerCommitMessageCount;
                if (topicServerCommitMessageTimestampUtc == null || topicServerCommitMessageTimestampUtc < partitionServerCommitMessageTimestampUtc)
                {
                    topicServerCommitMessageTimestampUtc = partitionServerCommitMessageTimestampUtc;
                }

                if (partitionIsAssigned)
                {                    
                    topicIsReady = (topicIsReady ?? true) && partitionIsReady;
                    topicIsSynchronized = topicIsSynchronized && partitionIsSynchronized;
                    topicReceivePendingCount += partitionReceivePendingCount ?? 0;
                    topicConsumePendingCount += partitionConsumePendingCount;
                    topicClientCommitPendingCount += partitionClientCommitPendingCount;
                    topicServerCommitPendingCount += partitionServerCommitPendingCount;
                }

                var partitionMessageCountInfo = new KafkaConsumerTopicMessageCountInfo(
                    partitionReceivePendingCount, partitionTotalReceivedCount, partitionReceivedTimestampUtc,
                    partitionConsumePendingCount, partitionConsumeMessageCount, partitionConsumeMessageTimestampUtc,
                    partitionClientCommitPendingCount, partitionClientCommitMessageCount, partitionClientCommitMessageTimestampUtc,
                    partitionServerCommitPendingCount, partitionServerCommitMessageCount, partitionServerCommitMessageTimestampUtc);

                var partitionTotalReceivedMessageSizeBytes = partitionBroker.TotalReceivedMessageSizeBytes;
                var partitionTotalConsumedMessageSizeBytes = partitionBroker.TotalConsumedMessageSizeBytes;
                var partitionConsumePendingMessageSizeBytes = partitionBroker.ConsumePendingMessageSizeBytes;
                var partitionBufferedMessageSizeBytes = partitionBroker.BufferedMessageSizeBytes;

                topicTotalReceivedMessageSizeBytes += partitionBufferedMessageSizeBytes;
                topicTotalConsumedMessageSizeBytes += partitionTotalConsumedMessageSizeBytes;
                topicConsumePendingMessageSizeBytes += partitionConsumePendingMessageSizeBytes;
                topicBufferedMessageSizeBytes += partitionBufferedMessageSizeBytes;

                var partitionMessageSizeInfo = new KafkaConsumerTopicMessageSizeInfo(
                    partitionTotalReceivedMessageSizeBytes, partitionTotalConsumedMessageSizeBytes, 
                    partitionConsumePendingMessageSizeBytes, partitionBufferedMessageSizeBytes);

                var partitionInfo = new KafkaConsumerTopicPartitionInfo(partition.PartitonId,
                    partitionIsAssigned, partitionIsReady, partitionIsSynchronized,
                    partitionBroker.Error, partitionBroker.ErrorTimestampUtc,
                    partitionMessageCountInfo,
                    partitionMessageSizeInfo,
                    partitionOffsetsInfo);
                partitionInfos.Add(partitionInfo);
            }

            var metadataInfo = TopicMetadataInfo;

            var topicMessageCountInfo = new KafkaConsumerTopicMessageCountInfo(
                topicReceivePendingCount, topicTotalReceivedMessageCount, topicReceiveMessageTimestampUtc,
                topicConsumePendingCount, topicTotalConsumedMessageCount, topicConsumeMessageTimestampUtc,
                topicClientCommitPendingCount, topicTotalClientCommitedMessageCount, topicClientCommitMessageTimestampUtc,
                topicServerCommitPendingCount, topicTotalServerCommitedMessageCount, topicServerCommitMessageTimestampUtc);

            var topicMessageSizeInfo = new KafkaConsumerTopicMessageSizeInfo(
                topicTotalReceivedMessageSizeBytes, topicTotalConsumedMessageSizeBytes, topicConsumePendingMessageSizeBytes, topicBufferedMessageSizeBytes);

            var consumerGroupInfo = _group?.GroupCoordinator?.GetDiagnosticsInfo();
            var catchUpGroupInfo = _group?.CatchUpGroupCoordinator?.GetDiagnosticsInfo();

            topicIsReady = (topicIsReady == true) && metadataInfo.IsReady && (consumerGroupInfo?.IsReady == true);
            if (_group?.CatchUpGroupCoordinator != null)
            {
                topicIsReady = (topicIsReady == true) && (catchUpGroupInfo?.IsReady == true);
            }

            return new KafkaConsumerTopicInfo(TopicName, 
                topicIsReady == true, topicIsSynchronized,
                metadataInfo, consumerGroupInfo, catchUpGroupInfo,
                topicMessageCountInfo,
                topicMessageSizeInfo,
                partitionInfos,
                DateTime.UtcNow);
        }

        #endregion Diagnostics

        private class KafkaConsumerTopicPackageInfo
        {
            public readonly int PartitionId;

            public readonly long BeginOffset;

            public readonly long EndOffset;            

            public KafkaConsumerTopicPackageInfo(int partitionId, long beginOffset, long endOffset)
            {
                PartitionId = partitionId;
                BeginOffset = beginOffset;
                EndOffset = endOffset;
            }
        }        
    }
}