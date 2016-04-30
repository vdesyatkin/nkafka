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
                count += partition.EnqueuedCount;
            }

            if (count == 0) return null;

            var packagePartitions = new List<PackagePartitionInfo>(_topicPartitions.Count);

            var messages = new List<KafkaMessage>(count);
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionEnqueuedCount = partition.EnqueuedCount;
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

        public void SetCommitServerOffset(int partitionId, long offset)
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

            int enqueuedMessageCount = 0;
            long totalReceivedMessageCount = 0;
            long totalConsumedMessageCount = 0;
            long totalCommitedMessageCount = 0;
            var receiveMessageTimestampUtc = (DateTime?)null;
            var consumeMessageTimestampUtc = (DateTime?)null;
            var commitMessageTimestampUtc = (DateTime?)null;

            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;

                var partitionEnqueuedCount = partition.EnqueuedCount;
                var partitionTotalEnqueuedCount = partition.TotalEnqueuedCount;
                var partitionEnqueueTimestampUtc = partition.EnqueueTimestampUtc;
                var partitionConsumeMessageCount = partition.TotalConsumedCount;
                var partitionConsumeMessageTimestampUtc = partition.ConsumeTimestampUtc;
                var partitionCommitMessageCount = partition.TotalCommitedCount;
                var partitionCommitMessageTimestampUtc = partition.CommitTimestampUtc;

                enqueuedMessageCount += partitionEnqueuedCount;
                totalReceivedMessageCount += partitionTotalEnqueuedCount;
                if (receiveMessageTimestampUtc == null || receiveMessageTimestampUtc < partitionEnqueueTimestampUtc)
                {
                    receiveMessageTimestampUtc = partitionEnqueueTimestampUtc;
                }

                totalConsumedMessageCount += partitionConsumeMessageCount;
                if (consumeMessageTimestampUtc == null || consumeMessageTimestampUtc < partitionConsumeMessageTimestampUtc)
                {
                    consumeMessageTimestampUtc = partitionConsumeMessageTimestampUtc;
                }

                totalCommitedMessageCount += partitionCommitMessageCount;
                if (commitMessageTimestampUtc == null || commitMessageTimestampUtc < partitionCommitMessageTimestampUtc)
                {
                    commitMessageTimestampUtc = partitionCommitMessageTimestampUtc;
                }

                var partitionMessagesInfo = new KafkaConsumerTopicMessagesInfo(
                    partitionEnqueuedCount, partitionTotalEnqueuedCount, partitionEnqueueTimestampUtc,
                    partitionConsumeMessageCount, partitionConsumeMessageTimestampUtc,
                    partitionCommitMessageCount, partitionCommitMessageTimestampUtc);

                var partitionInfo = new KafkaConsumerTopicPartitionInfo(partition.PartitonId,
                    partitionBroker.IsReady,
                    partitionBroker.Error, partitionBroker.ErrorTimestampUtc,
                    partitionMessagesInfo); //todo (E008) Offsets info
                partitionInfos.Add(partitionInfo);
            }            

            var metadataInfo = TopicMetadataInfo;

            var topicMessagesInfo = new KafkaConsumerTopicMessagesInfo(
                enqueuedMessageCount, totalReceivedMessageCount, receiveMessageTimestampUtc,
                totalConsumedMessageCount, commitMessageTimestampUtc,
                totalCommitedMessageCount, receiveMessageTimestampUtc);

            return new KafkaConsumerTopicInfo(TopicName, 
                metadataInfo, 
                topicMessagesInfo,
                partitionInfos, 
                DateTime.UtcNow); //todo (E008) message count to highwatermark
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