using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic : IKafkaConsumerTopic
    {
        [NotNull] public readonly string TopicName;
        [CanBeNull] public readonly IKafkaConsumerGroup Group;

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull] private IReadOnlyDictionary<int, KafkaConsumerTopicPartition> _topicPartitions;
        [NotNull] private readonly ConcurrentDictionary<int, PackageInfo> _packages;
        private int _currentPackageId;

        public KafkaConsumerTopic([NotNull] string topicName, [CanBeNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerSettings settings)
        { 
            TopicName = topicName;
            Group = group;
            Settings = settings;
            _topicPartitions = new Dictionary<int, KafkaConsumerTopicPartition>();
            _packages = new ConcurrentDictionary<int, PackageInfo>();            
        }

        public KafkaConsumerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaConsumerTopicPartition(TopicName, partitionId, Settings);
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
        
        public KafkaMessagePackage Consume()
        {
            var count = 0;
            foreach (var partition in _topicPartitions)
            {
                count += partition.Value.EnqueuedCount;
            }

            if (count == 0) return null;

            var partitionOffsets = new Dictionary<int, long>();

            var messages = new List<KafkaMessage>(count);
            foreach (var partition in _topicPartitions)
            {
                var partitionCount = partition.Value.EnqueuedCount;
                KafkaMessageAndOffset messageAndOffset = null;
                while (partitionCount > 0 && partition.Value.TryDequeue(out messageAndOffset))
                {
                    var message = new KafkaMessage(messageAndOffset.Key, messageAndOffset.Data);
                    messages.Add(message);
                    partitionCount--;
                }
                if (messageAndOffset != null)
                {
                    partitionOffsets[partition.Key] = messageAndOffset.Offset;
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
            if (!_packages.TryGetValue(packageNumber, out package))
            {
                return;
            }

            foreach (var partitionOffset in package.PartitionOffsets)
            {
                var partitionId = partitionOffset.Key;
                var newOffset = partitionOffset.Value;

                KafkaConsumerTopicPartition partition;
                if (!_topicPartitions.TryGetValue(partitionId, out partition))
                {
                    continue;
                }

                partition.CommitOffset(newOffset);                
            }

            _packages.TryRemove(packageNumber, out package);
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
    }
}