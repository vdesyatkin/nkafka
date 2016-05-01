using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic : IKafkaConsumerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly string GroupName;
        [NotNull] public KafkaClientTopicMetadataInfo TopicMetadataInfo;

        [CanBeNull] private IKafkaConsumerCoordinator _coordinator;

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull, ItemNotNull] private IReadOnlyDictionary<int, KafkaConsumerTopicPartition> _topicPartitions;
        [NotNull] private readonly ConcurrentDictionary<long, PackageInfo> _packages;
        private long _currentPackageId;

        public KafkaConsumerTopic([NotNull] string topicName, [NotNull] string groupName, [NotNull] KafkaConsumerSettings settings)
        { 
            TopicName = topicName;
            GroupName = groupName;
            Settings = settings;
            _topicPartitions = new Dictionary<int, KafkaConsumerTopicPartition>();
            _packages = new ConcurrentDictionary<long, PackageInfo>();
            TopicMetadataInfo = new KafkaClientTopicMetadataInfo(topicName, DateTime.UtcNow, false, null, null);
        }

        [CanBeNull]
        public KafkaConsumerTopicPartition CreatePartition(int partitionId)
        {
            var coordinator = _coordinator;
            return coordinator == null ? null : new KafkaConsumerTopicPartition(TopicName, partitionId, Settings, coordinator);
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

        public void ApplyCoordinator([NotNull] IKafkaConsumerCoordinator coordinator)
        {
            _coordinator = coordinator;
        }
        
        public KafkaMessagePackage Consume(int? maxMessageCount = null)
        {
            int count = 0;
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                count += partition.ConsumePendingCount;
            }

            if (count == 0) return null;

            var packagePartitions = new List<PackagePartitionInfo>(_topicPartitions.Count);

            var messages = new List<KafkaMessage>(count);
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionEnqueuedCount = partition.ConsumePendingCount;
                var partitionPackageCount = 0;
                KafkaMessageAndOffset messageAndOffset = null;
                while (partitionPackageCount < partitionEnqueuedCount && partition.TryDequeue(out messageAndOffset))
                {
                    if (messageAndOffset != null)
                    {
                        var message = new KafkaMessage(messageAndOffset.Key, messageAndOffset.Data);
                        messages.Add(message);
                    }                
                    
                    partitionPackageCount++;
                    if (messages.Count >= maxMessageCount)
                    {
                        break;
                    }
                }
                if (messageAndOffset != null)
                {
                    packagePartitions[partitionPair.Key] = new PackagePartitionInfo(partition.PartitonId, messageAndOffset.Offset, partitionPackageCount);
                }

                if (messages.Count >= maxMessageCount)
                {
                    break;
                }
            }

            if (messages.Count == 0) return null;
            var packageId = Interlocked.Increment(ref _currentPackageId);

            var packageInfo = new PackageInfo(packagePartitions);
            _packages[packageId] = packageInfo;

            return new KafkaMessagePackage(packageId, messages);
        }

        public void EnqueueCommit(long packageNumber)
        {
            PackageInfo package;
            if (!_packages.TryGetValue(packageNumber, out package) || package == null)
            {
                return;
            }

            foreach (var partitionData in package.Partitions)
            {                
                KafkaConsumerTopicPartition partition;
                if (!_topicPartitions.TryGetValue(partitionData.PartitionId, out partition) || partition == null)
                {
                    continue;
                }

                partition.SetCommitClientOffset(partitionData.LastOffset, partitionData.Count);
            }

            _packages.TryRemove(packageNumber, out package);
        }

        public void SetCommitServerOffset(int partitionId, long? offset)
        {
            KafkaConsumerTopicPartition partition;
            if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null) return;

            partition.SetCommitServerOffset(offset);
        }

        public long? GetCommitClientOffset(int partitionId)
        {
            KafkaConsumerTopicPartition partition;
            if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null) return null;
            return partition.GetCommitClientOffset();
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
            
            //todo (E008) size statistics            
            //todo (E008) total ready
            //todo (E008) only assigned
            foreach (var partitionPair in _topicPartitions) 
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;

                var partitionOffsetsInfo = partitionBroker.GetOffsetsInfo();

                var partitionReceivePendingCount = partitionOffsetsInfo.MaxAvailableOffset - (partitionOffsetsInfo.ReceivedOffset ?? partitionOffsetsInfo.CommitedServerOffset);
                if (partitionReceivePendingCount < 0)
                {
                    partitionReceivePendingCount = null;
                }
                var partitionTotalReceivedCount = partition.TotalReceivedCount;
                var partitionReceivedTimestampUtc = partition.ReceiveTimestampUtc;

                var partitionConsumePendingCount = partition.ConsumePendingCount;
                var partitionConsumeMessageCount = partition.TotalConsumedCount;
                var partitionConsumeMessageTimestampUtc = partition.ConsumeTimestampUtc;

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
                var partitionServerCommitMessageTimestampUtc = partition.ServerCommitTimestampUtc;
                                               
                topicReceivePendingCount += partitionReceivePendingCount ?? 0;
                topicConsumePendingCount += topicConsumePendingCount;
                topicTotalReceivedMessageCount += partitionTotalReceivedCount;
                if (topicReceiveMessageTimestampUtc == null || topicReceiveMessageTimestampUtc < partitionReceivedTimestampUtc)
                {
                    topicReceiveMessageTimestampUtc = partitionReceivedTimestampUtc;
                }

                topicConsumePendingCount += partitionConsumePendingCount;
                topicTotalConsumedMessageCount += partitionConsumeMessageCount;
                if (topicConsumeMessageTimestampUtc == null || topicConsumeMessageTimestampUtc < partitionConsumeMessageTimestampUtc)
                {
                    topicConsumeMessageTimestampUtc = partitionConsumeMessageTimestampUtc;
                }

                topicClientCommitPendingCount += partitionClientCommitPendingCount;
                topicTotalClientCommitedMessageCount += partitionClientCommitMessageCount;
                if (topicClientCommitMessageTimestampUtc == null || topicClientCommitMessageTimestampUtc < partitionClientCommitMessageTimestampUtc)
                {
                    topicClientCommitMessageTimestampUtc = partitionClientCommitMessageTimestampUtc;
                }

                topicServerCommitPendingCount += partitionServerCommitPendingCount;
                topicTotalServerCommitedMessageCount += partitionServerCommitMessageCount;
                if (topicServerCommitMessageTimestampUtc == null || topicServerCommitMessageTimestampUtc < partitionServerCommitMessageTimestampUtc)
                {
                    topicServerCommitMessageTimestampUtc = partitionServerCommitMessageTimestampUtc;
                }

                var partitionMessagesInfo = new KafkaConsumerTopicMessagesInfo(
                    partitionReceivePendingCount, partitionTotalReceivedCount, partitionReceivedTimestampUtc,
                    partitionConsumePendingCount, partitionConsumeMessageCount, partitionConsumeMessageTimestampUtc,
                    partitionClientCommitPendingCount, partitionClientCommitMessageCount, partitionClientCommitMessageTimestampUtc,
                    partitionServerCommitPendingCount, partitionServerCommitMessageCount, partitionServerCommitMessageTimestampUtc);

                var partitionInfo = new KafkaConsumerTopicPartitionInfo(partition.PartitonId,
                    partitionBroker.IsReady,
                    partitionBroker.Error, partitionBroker.ErrorTimestampUtc,
                    partitionMessagesInfo,
                    partitionOffsetsInfo);
                partitionInfos.Add(partitionInfo);
            }

            var metadataInfo = TopicMetadataInfo;

            var topicMessagesInfo = new KafkaConsumerTopicMessagesInfo(
                topicReceivePendingCount, topicTotalReceivedMessageCount, topicReceiveMessageTimestampUtc,
                topicConsumePendingCount, topicTotalConsumedMessageCount, topicConsumeMessageTimestampUtc,
                topicClientCommitPendingCount, topicTotalClientCommitedMessageCount, topicClientCommitMessageTimestampUtc,
                topicServerCommitPendingCount, topicTotalServerCommitedMessageCount, topicServerCommitMessageTimestampUtc);

            return new KafkaConsumerTopicInfo(TopicName, 
                metadataInfo, 
                topicMessagesInfo,
                partitionInfos, 
                DateTime.UtcNow);
        }

        private class PackageInfo
        {
            [NotNull]
            public readonly IReadOnlyList<PackagePartitionInfo> Partitions;

            public PackageInfo([NotNull] IReadOnlyList<PackagePartitionInfo> partitions)
            {
                Partitions = partitions;
            }
        }

        private struct PackagePartitionInfo
        {
            public readonly int PartitionId;

            public readonly long LastOffset;

            public readonly int Count;

            public PackagePartitionInfo(int partitionId, long lastOffset, int count)
            {
                PartitionId = partitionId;
                LastOffset = lastOffset;
                Count = count;
            }
        }      
    }
}