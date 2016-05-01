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
        
        public IReadOnlyList<KafkaMessagePackage> Consume(int? maxMessageCount = null)
        {            
            var resultPackages = new List<KafkaMessagePackage>(_topicPartitions.Count);
            var totalMessageCount = 0;
            
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionConsumePendingCount = partition.ConsumePendingCount;                
                var partitionMessages = new List<KafkaMessage>(partitionConsumePendingCount);

                long beginOffset = 0;
                long endOffset = 0;

                KafkaMessageAndOffset messageAndOffset;
                while (partitionMessages.Count < partitionConsumePendingCount && partition.TryDequeue(out messageAndOffset))
                {
                    if (messageAndOffset == null) continue;                    
                    
                    var message = new KafkaMessage(messageAndOffset.Key, messageAndOffset.Data);

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
                    _packages[packageId] = new PackageInfo(partition.PartitonId, beginOffset, endOffset);
                    resultPackages.Add(new KafkaMessagePackage(packageId, partitionMessages));
                }

                if (totalMessageCount >= maxMessageCount)
                {
                    break;
                }
            }

            return resultPackages;
        }

        public bool TryEnqueueCommit(long packageId)
        {
            PackageInfo package;
            if (!_packages.TryGetValue(packageId, out package) || package == null)
            {
                return true;
            }

            KafkaConsumerTopicPartition partition;
            if (_topicPartitions.TryGetValue(package.PartitionId, out partition) && partition != null)
            {
                if (!partition.IsAssigned) return false;
                partition.SetCommitClientOffset(package.BeginOffset, package.EndOffset);
            }

            _packages.TryRemove(packageId, out package);            
            return true;
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

            var topicIsReady = true;

            //todo (E008) size statistics
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;
                
                var partitionIsAssigned = partition.IsAssigned;

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
                    topicIsReady = topicIsReady && partitionBroker.IsReady;
                    topicReceivePendingCount += partitionReceivePendingCount ?? 0;
                    topicConsumePendingCount += partitionConsumePendingCount;
                    topicClientCommitPendingCount += partitionClientCommitPendingCount;
                    topicServerCommitPendingCount += partitionServerCommitPendingCount; //todo (E009) the main kafka problem =( some fallback on partition unassign?
                }

                var partitionMessagesInfo = new KafkaConsumerTopicMessagesInfo(
                    partitionReceivePendingCount, partitionTotalReceivedCount, partitionReceivedTimestampUtc,
                    partitionConsumePendingCount, partitionConsumeMessageCount, partitionConsumeMessageTimestampUtc,
                    partitionClientCommitPendingCount, partitionClientCommitMessageCount, partitionClientCommitMessageTimestampUtc,
                    partitionServerCommitPendingCount, partitionServerCommitMessageCount, partitionServerCommitMessageTimestampUtc);

                var partitionInfo = new KafkaConsumerTopicPartitionInfo(partition.PartitonId,
                    partitionIsAssigned, partitionBroker.IsReady,
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

            topicIsReady = topicIsReady && metadataInfo.IsReady;

            return new KafkaConsumerTopicInfo(TopicName, 
                topicIsReady,
                metadataInfo, 
                topicMessagesInfo,
                partitionInfos,
                DateTime.UtcNow);
        }

        private class PackageInfo
        {
            public readonly int PartitionId;

            public readonly long BeginOffset;

            public readonly long EndOffset;            

            public PackageInfo(int partitionId, long beginOffset, long endOffset)
            {
                PartitionId = partitionId;
                BeginOffset = beginOffset;
                EndOffset = endOffset;
            }
        }        
    }
}