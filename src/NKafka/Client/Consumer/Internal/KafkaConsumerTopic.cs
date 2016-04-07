using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic : IKafkaConsumerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly string GroupName;

        [NotNull] private IKafkaConsumerCoordinator _coordinator;

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull, ItemNotNull] private IReadOnlyDictionary<int, KafkaConsumerTopicPartition> _topicPartitions;
        [NotNull] private readonly ConcurrentDictionary<int, PackageInfo> _packages;
        private int _currentPackageId;

        public KafkaConsumerTopic([NotNull] string topicName, [NotNull] string groupName, [NotNull] KafkaConsumerSettings settings)
        { 
            TopicName = topicName;
            GroupName = groupName;
            Settings = settings;
            _topicPartitions = new Dictionary<int, KafkaConsumerTopicPartition>();
            _packages = new ConcurrentDictionary<int, PackageInfo>();
            _coordinator = new NullCoordinator();
        }

        [NotNull]
        public KafkaConsumerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaConsumerTopicPartition(TopicName, partitionId, Settings, _coordinator);
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
        
        public KafkaMessagePackage Consume()
        {
            var count = 0;
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                count += partition.EnqueuedCount;
            }

            if (count == 0) return null;

            var partitionOffsets = new Dictionary<int, long>();

            var messages = new List<KafkaMessage>(count);
            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionCount = partition.EnqueuedCount;
                KafkaMessageAndOffset messageAndOffset = null;
                while (partitionCount > 0 && partition.TryDequeue(out messageAndOffset))
                {
                    if (messageAndOffset != null)
                    {
                        var message = new KafkaMessage(messageAndOffset.Key, messageAndOffset.Data, messageAndOffset.TimestampUtc);
                        messages.Add(message);
                    }                    
                    partitionCount--;
                }
                if (messageAndOffset != null)
                {
                    partitionOffsets[partitionPair.Key] = messageAndOffset.Offset;
                }
            }

            if (messages.Count == 0) return null;
            var packageId = Interlocked.Increment(ref _currentPackageId);

            var packageInfo = new PackageInfo(partitionOffsets);
            _packages[packageId] = packageInfo;

            return new KafkaMessagePackage(packageId, messages);
        }

        public void Commit(int packageNumber)
        {
            PackageInfo package;
            if (!_packages.TryGetValue(packageNumber, out package) || package == null)
            {
                return;
            }

            foreach (var partitionOffset in package.PartitionOffsets)
            {
                var partitionId = partitionOffset.Key;
                var newOffset = partitionOffset.Value;

                KafkaConsumerTopicPartition partition;
                if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                partition.RequestCommitOffset(newOffset);                
            }

            _packages.TryRemove(packageNumber, out package);
        }

        public void ApproveCommitOffset(int partitionId, long offset)
        {
            KafkaConsumerTopicPartition partition;
            if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null) return;

            partition.ApproveCommitOffset(offset);
        }

        public long? GetCommitOffset(int partitionId)
        {
            KafkaConsumerTopicPartition partition;
            if (!_topicPartitions.TryGetValue(partitionId, out partition) || partition == null) return null;
            return partition.GetCommitOffset();
        }

        private class PackageInfo
        {
            [NotNull]
            public readonly Dictionary<int, long> PartitionOffsets;

            public PackageInfo([NotNull] Dictionary<int, long> partitionOffsets)
            {
                PartitionOffsets = partitionOffsets;
            }
        }

        private class NullCoordinator : IKafkaConsumerCoordinator
        {
            public IReadOnlyDictionary<int, long?> GetPartitionOffsets(string topicName)
            {
                return new Dictionary<int, long?>();
            }
        }
    }
}